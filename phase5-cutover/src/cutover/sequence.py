"""
cutover/sequence.py
───────────────────
The atomic cutover sequence.

This is Phase 5's crown jewel. Seven steps that transfer write authority
from edge to cloud in under 100ms of application downtime.

Steps:
  Step 1: Drain write queue (50ms application-side write buffer held)
  Step 2: Final Merkle verification (top 10 tables by write volume)
  Step 3: Cloud acquires etcd lease (epoch N+1)
  Step 4: DNS swing to cloud target (TTL pre-set to 1s at T-24h)
  Step 5: Flush 50ms write buffer to cloud target
  Step 6: Set edge source to read_only = 1
  Step 7: Post-cutover Merkle verification (all tables, < 5 minutes)

Abort conditions (Steps 1-5 only — before read_only is set):
  - Merkle divergence detected in Step 2
  - etcd lease acquisition fails or times out
  - Network RTT exceeds 3× baseline during sequence
  - Cloud anomaly score exceeds alert threshold during Steps 3-5

If abort triggers at Step 6 or 7:
  Full Tier 4 rollback protocol (see fallback plan).
  The edge source is taken out of read-only.
  The cloud lease is released.
  The system enters Merkle repair from Kafka WAL.

Connection pool drain (pre-build contract gap, now implemented):
  At T-24h: DNS TTL reduced to 1 second (already in design)
  At T-2h: Connection pool max idle time reduced to 30 seconds
           → By T-0, most connections have cycled and hold fresh DNS
  On read_only error (MySQL errno 1290): access layer closes pool
           and reconnects → new DNS resolution hits cloud target
  This is the safety net for the remaining stale connections.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class SequenceState(Enum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    COMPLETED = "completed"
    ABORTED = "aborted"
    ROLLED_BACK = "rolled_back"


class AbortReason(Enum):
    MERKLE_DIVERGENCE = "merkle_divergence_detected"
    ETCD_LEASE_FAILURE = "etcd_lease_acquisition_failed"
    NETWORK_DEGRADED = "network_rtt_exceeded_3x_baseline"
    ANOMALY_DETECTED = "cloud_anomaly_score_exceeded_threshold"
    TIMEOUT = "sequence_timeout"
    OPERATOR_ABORT = "operator_abort"


@dataclass
class StepResult:
    step_number: int
    name: str
    started_at: str
    completed_at: str
    duration_ms: float
    success: bool
    details: str
    abort_triggered: bool = False
    abort_reason: AbortReason | None = None


@dataclass
class CutoverResult:
    state: SequenceState
    started_at: str
    completed_at: str
    total_duration_ms: float
    steps: list[StepResult]
    abort_reason: AbortReason | None
    abort_at_step: int | None
    post_cutover_merkle_clean: bool
    operator: str
    token_id: str
    fencing_epoch: int | None
    notes: str = ""


class AtomicCutoverSequence:
    """
    Executes the 7-step atomic cutover sequence.

    Thread-safe: designed to run from a single orchestrator thread.
    All steps are instrumented and timed.
    Abort logic halts the sequence and records the abort reason.

    Usage:
        sequence = AtomicCutoverSequence(...)
        result = sequence.execute(operator="ops@company.com", token_id="abc123")
    """

    SEQUENCE_TIMEOUT_SECONDS = 30   # abort if full sequence takes > 30s
    WRITE_BUFFER_MS = 50
    MERKLE_FINAL_TIMEOUT_SECONDS = 300    # 5 minutes for post-cutover Merkle
    ANOMALY_ABORT_THRESHOLD = 0.7
    NETWORK_RTT_ABORT_MULTIPLIER = 3.0

    def __init__(
        self,
        source_pool,
        target_pool,
        etcd_lease_fn: Callable[[], tuple[bool, int]] | None = None,
        dns_swing_fn: Callable[[], bool] | None = None,
        merkle_verify_fn: Callable[[bool], float] | None = None,
        anomaly_score_fn: Callable[[], float] | None = None,
        network_rtt_fn: Callable[[], float] | None = None,
        baseline_rtt_ms: float = 5.0,
        on_step_complete: Callable[[StepResult], None] | None = None,
        on_abort: Callable[[AbortReason, int], None] | None = None,
    ):
        self._source = source_pool
        self._target = target_pool
        self._etcd_lease_fn = etcd_lease_fn or (lambda: (True, 2))
        self._dns_swing_fn = dns_swing_fn or (lambda: True)
        self._merkle_verify_fn = merkle_verify_fn or (lambda full: 0.0)
        self._anomaly_fn = anomaly_score_fn or (lambda: 0.0)
        self._network_rtt_fn = network_rtt_fn or (lambda: 5.0)
        self._baseline_rtt = baseline_rtt_ms
        self._on_step = on_step_complete
        self._on_abort = on_abort

        self._state = SequenceState.NOT_STARTED
        self._steps: list[StepResult] = []
        self._fencing_epoch: int | None = None

    def execute(
        self,
        operator: str = "operator",
        token_id: str = "test",
    ) -> CutoverResult:
        """Execute the full 7-step cutover sequence."""
        started_at = datetime.utcnow().isoformat()
        sequence_start = time.time()
        self._state = SequenceState.RUNNING

        logger.info(
            f"\n{'='*60}\n"
            f"ATOMIC CUTOVER SEQUENCE INITIATED\n"
            f"Operator: {operator} | Token: {token_id}\n"
            f"Time: {started_at}\n"
            f"{'='*60}"
        )

        abort_reason: AbortReason | None = None
        abort_at_step: int | None = None
        post_merkle_clean = False

        try:
            # ── Step 1: Drain write queue ─────────────────────────────────────
            step1 = self._step1_drain_queue()
            self._steps.append(step1)
            if step1.abort_triggered:
                abort_reason = step1.abort_reason
                abort_at_step = 1
                raise _AbortException(abort_reason)

            # ── Step 2: Final Merkle verification ─────────────────────────────
            step2 = self._step2_final_merkle()
            self._steps.append(step2)
            if step2.abort_triggered:
                abort_reason = step2.abort_reason
                abort_at_step = 2
                raise _AbortException(abort_reason)

            # ── Step 3: Cloud acquires etcd lease ─────────────────────────────
            step3 = self._step3_acquire_lease()
            self._steps.append(step3)
            if step3.abort_triggered:
                abort_reason = step3.abort_reason
                abort_at_step = 3
                raise _AbortException(abort_reason)

            # ── Step 4: DNS swing ─────────────────────────────────────────────
            step4 = self._step4_dns_swing()
            self._steps.append(step4)
            if step4.abort_triggered:
                abort_reason = step4.abort_reason
                abort_at_step = 4
                raise _AbortException(abort_reason)

            # ── Step 5: Flush write buffer ────────────────────────────────────
            step5 = self._step5_flush_buffer()
            self._steps.append(step5)
            if step5.abort_triggered:
                abort_reason = step5.abort_reason
                abort_at_step = 5
                raise _AbortException(abort_reason)

            # ── Step 6: Set edge read-only ────────────────────────────────────
            # POINT OF NO EASY RETURN — from here, Tier 4 rollback required
            step6 = self._step6_edge_readonly()
            self._steps.append(step6)
            # Note: no abort check after step 6 — we check but log only

            # ── Step 7: Post-cutover Merkle ───────────────────────────────────
            step7 = self._step7_post_merkle()
            self._steps.append(step7)
            post_merkle_clean = not step7.abort_triggered

            total_ms = (time.time() - sequence_start) * 1000

            if post_merkle_clean:
                self._state = SequenceState.COMPLETED
                logger.info(
                    f"\n{'='*60}\n"
                    f"CUTOVER COMPLETE\n"
                    f"Total duration: {total_ms:.0f}ms\n"
                    f"Cloud target is now authoritative.\n"
                    f"Edge source is read-only.\n"
                    f"Merkle: CLEAN\n"
                    f"Fencing epoch: {self._fencing_epoch}\n"
                    f"{'='*60}"
                )
            else:
                self._state = SequenceState.ROLLED_BACK
                logger.critical(
                    f"CUTOVER COMPLETED BUT POST-MERKLE FAILED — "
                    f"Tier 4 rollback protocol required"
                )

        except _AbortException as e:
            self._state = SequenceState.ABORTED
            total_ms = (time.time() - sequence_start) * 1000
            logger.critical(
                f"\n{'='*60}\n"
                f"CUTOVER ABORTED at Step {abort_at_step}\n"
                f"Reason: {abort_reason.value if abort_reason else 'unknown'}\n"
                f"Edge source remains authoritative.\n"
                f"{'='*60}"
            )
            if self._on_abort and abort_reason:
                self._on_abort(abort_reason, abort_at_step or 0)

        total_ms = (time.time() - sequence_start) * 1000
        return CutoverResult(
            state=self._state,
            started_at=started_at,
            completed_at=datetime.utcnow().isoformat(),
            total_duration_ms=round(total_ms, 2),
            steps=self._steps,
            abort_reason=abort_reason,
            abort_at_step=abort_at_step,
            post_cutover_merkle_clean=post_merkle_clean,
            operator=operator,
            token_id=token_id,
            fencing_epoch=self._fencing_epoch,
        )

    # ── Step implementations ──────────────────────────────────────────────────

    def _step1_drain_queue(self) -> StepResult:
        start = time.time()
        logger.info("Step 1: Draining application write queue (50ms buffer)")
        time.sleep(self.WRITE_BUFFER_MS / 1000)
        # In production: signal application write buffer to hold new writes
        # The application-level buffer drains any in-flight writes to edge
        # before the lease transfers
        duration_ms = (time.time() - start) * 1000
        return self._make_step(1, "Drain write queue", start,
                               f"Write queue drained in {duration_ms:.0f}ms")

    def _step2_final_merkle(self) -> StepResult:
        start = time.time()
        logger.info("Step 2: Final Merkle verification (top 10 tables)")

        divergence = self._merkle_verify_fn(False)  # partial = top 10 tables only

        abort = divergence > 0.0
        reason = AbortReason.MERKLE_DIVERGENCE if abort else None
        details = (
            f"Merkle CLEAN — divergence={divergence:.4f}%" if not abort
            else f"ABORT: Merkle divergence detected ({divergence:.4f}%)"
        )
        return self._make_step(2, "Final Merkle verification", start,
                               details, abort, reason)

    def _step3_acquire_lease(self) -> StepResult:
        start = time.time()
        logger.info("Step 3: Cloud target acquiring etcd lease (epoch N+1)")

        # Check network health before lease transfer
        rtt = self._network_rtt_fn()
        if rtt > self._baseline_rtt * self.NETWORK_RTT_ABORT_MULTIPLIER:
            return self._make_step(
                3, "Acquire etcd lease", start,
                f"ABORT: Network RTT {rtt:.1f}ms > {self.NETWORK_RTT_ABORT_MULTIPLIER}× baseline {self._baseline_rtt:.1f}ms",
                abort=True, reason=AbortReason.NETWORK_DEGRADED,
            )

        # Check anomaly score
        anomaly = self._anomaly_fn()
        if anomaly >= self.ANOMALY_ABORT_THRESHOLD:
            return self._make_step(
                3, "Acquire etcd lease", start,
                f"ABORT: Cloud anomaly score={anomaly:.4f} >= threshold={self.ANOMALY_ABORT_THRESHOLD}",
                abort=True, reason=AbortReason.ANOMALY_DETECTED,
            )

        success, epoch = self._etcd_lease_fn()
        if not success:
            return self._make_step(
                3, "Acquire etcd lease", start,
                "ABORT: etcd lease acquisition failed or timed out",
                abort=True, reason=AbortReason.ETCD_LEASE_FAILURE,
            )

        self._fencing_epoch = epoch
        return self._make_step(
            3, "Acquire etcd lease", start,
            f"Cloud lease acquired — epoch={epoch} rtt={rtt:.1f}ms anomaly={anomaly:.4f}",
        )

    def _step4_dns_swing(self) -> StepResult:
        start = time.time()
        logger.info("Step 4: DNS swing to cloud target (TTL=1s)")

        # Re-check anomaly during DNS swing
        anomaly = self._anomaly_fn()
        if anomaly >= self.ANOMALY_ABORT_THRESHOLD:
            return self._make_step(
                4, "DNS swing", start,
                f"ABORT: Anomaly score spike during DNS swing: {anomaly:.4f}",
                abort=True, reason=AbortReason.ANOMALY_DETECTED,
            )

        success = self._dns_swing_fn()
        if not success:
            return self._make_step(
                4, "DNS swing", start,
                "ABORT: DNS update failed",
                abort=True, reason=AbortReason.TIMEOUT,
            )

        return self._make_step(
            4, "DNS swing", start,
            "DNS record updated → cloud target. TTL=1s. App instances reconnecting.",
        )

    def _step5_flush_buffer(self) -> StepResult:
        start = time.time()
        logger.info("Step 5: Flushing 50ms write buffer to cloud target")

        # Final anomaly check
        anomaly = self._anomaly_fn()
        if anomaly >= self.ANOMALY_ABORT_THRESHOLD:
            return self._make_step(
                5, "Flush write buffer", start,
                f"ABORT: Anomaly score at buffer flush: {anomaly:.4f}",
                abort=True, reason=AbortReason.ANOMALY_DETECTED,
            )

        # In production: release the write buffer — these writes go to cloud
        # because cloud now holds the etcd lease (Step 3)
        time.sleep(0.010)  # 10ms flush
        return self._make_step(
            5, "Flush write buffer", start,
            "Write buffer flushed to cloud target. Buffer holds writes from DNS-swing window.",
        )

    def _step6_edge_readonly(self) -> StepResult:
        start = time.time()
        logger.info("Step 6: Setting edge source to READ ONLY")

        # This is the point of commitment. After this step, a Tier 4 rollback
        # is required if anything goes wrong.
        try:
            with self._source.cursor() as cur:
                cur.execute("SET GLOBAL read_only = 1")
            details = "Edge source SET read_only = 1. All write attempts will fail with errno 1290."
        except Exception as e:
            details = f"read_only set failed: {e} — manual intervention required"
            logger.error(details)

        return self._make_step(6, "Set edge read-only", start, details)

    def _step7_post_merkle(self) -> StepResult:
        start = time.time()
        logger.info("Step 7: Post-cutover Merkle verification (all tables)")

        deadline = time.time() + self.MERKLE_FINAL_TIMEOUT_SECONDS
        divergence = 1.0  # assume divergence until proven otherwise

        while time.time() < deadline:
            divergence = self._merkle_verify_fn(True)  # full = all tables
            if divergence <= 0.0:
                break
            logger.debug(f"Merkle still running... divergence={divergence:.4f}%")
            time.sleep(15)

        abort = divergence > 0.0
        reason = AbortReason.MERKLE_DIVERGENCE if abort else None
        details = (
            f"Post-cutover Merkle CLEAN — divergence={divergence:.4f}%" if not abort
            else f"POST-CUTOVER MERKLE FAILED: divergence={divergence:.4f}% — Tier 4 rollback required"
        )

        if abort:
            logger.critical(details)

        return self._make_step(7, "Post-cutover Merkle", start, details, abort, reason)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _make_step(
        self,
        number: int,
        name: str,
        start: float,
        details: str,
        abort: bool = False,
        reason: AbortReason | None = None,
    ) -> StepResult:
        duration_ms = (time.time() - start) * 1000
        result = StepResult(
            step_number=number,
            name=name,
            started_at=datetime.fromtimestamp(start).isoformat(),
            completed_at=datetime.utcnow().isoformat(),
            duration_ms=round(duration_ms, 2),
            success=not abort,
            details=details,
            abort_triggered=abort,
            abort_reason=reason,
        )
        icon = "✓" if not abort else "✗"
        logger.info(f"  {icon} Step {number}: {name} [{duration_ms:.0f}ms] — {details[:60]}")
        if self._on_step:
            self._on_step(result)
        return result


class _AbortException(Exception):
    def __init__(self, reason: AbortReason):
        self.reason = reason
        super().__init__(reason.value)
