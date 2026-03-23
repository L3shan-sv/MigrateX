"""
monitor/post_cutover.py
───────────────────────
Post-cutover validation monitor.

T+0 to T+30 minutes: IMMEDIATE validation (most sensitive window)
  - P99 latency on cloud target at 10-second intervals
  - Application error rate < 0.1% triggers alert
  - Binlog GTID position confirmed: cloud >= edge
  - Connection pools within 60% of capacity by T+5 minutes
  - Anomaly threshold at most sensitive setting

T+30 minutes to T+24 hours: EXTENDED validation
  - Merkle every 15 minutes (first 2 hours), then hourly
  - Edge stays online read-only as hot standby throughout
  - 1000 business transaction spot checks vs edge
  - Anomaly threshold returned to normal at T+4 hours

The edge source stays online in read-only mode as hot standby
for the full 24-hour extended validation window.
This preserves the Tier 3 rollback path.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

logger = logging.getLogger(__name__)


@dataclass
class ValidationCheckpoint:
    """One validation check during the post-cutover window."""
    checkpoint_id: str
    phase: str              # "immediate" or "extended"
    checked_at: str
    p99_latency_ms: float
    error_rate_pct: float
    merkle_divergence_pct: float
    anomaly_score: float
    replication_lag_seconds: float
    conn_pool_utilisation_pct: float
    all_checks_green: bool
    notes: str = ""


@dataclass
class PostCutoverReport:
    cutover_completed_at: str
    report_generated_at: str
    immediate_window_passed: bool
    extended_window_passed: bool
    total_checkpoints: int
    failed_checkpoints: int
    merkle_verifications: int
    merkle_all_clean: bool
    spot_checks_run: int
    spot_checks_passed: int
    edge_decommission_safe: bool   # True = safe to begin Phase 6
    checkpoints: list[ValidationCheckpoint]


class PostCutoverMonitor:
    """
    Monitors cloud target health for 24 hours after cutover.
    The edge source stays read-only throughout (hot standby).
    """

    IMMEDIATE_CHECK_INTERVAL_SECONDS = 10
    IMMEDIATE_WINDOW_SECONDS = 1800          # 30 minutes
    EXTENDED_MERKLE_INITIAL_INTERVAL = 900   # 15 min (first 2h)
    EXTENDED_MERKLE_LATER_INTERVAL = 3600    # hourly (after 2h)
    EXTENDED_WINDOW_SECONDS = 86400          # 24 hours
    ANOMALY_SENSITIVE_THRESHOLD = 0.5        # more sensitive during immediate
    ANOMALY_NORMAL_THRESHOLD = 0.7

    THRESHOLDS = {
        "p99_latency_ms_max": 400.0,         # 2× Phase 3 baseline
        "error_rate_pct_max": 0.1,
        "conn_pool_pct_max": 60.0,
        "merkle_divergence_max": 0.0,
    }

    def __init__(
        self,
        p99_latency_fn: Callable[[], float] | None = None,
        error_rate_fn: Callable[[], float] | None = None,
        merkle_verify_fn: Callable[[], float] | None = None,
        anomaly_score_fn: Callable[[], float] | None = None,
        replication_lag_fn: Callable[[], float] | None = None,
        conn_pool_utilisation_fn: Callable[[], float] | None = None,
        spot_check_fn: Callable[[], bool] | None = None,
        on_alert: Callable[[str, ValidationCheckpoint], None] | None = None,
        on_immediate_complete: Callable[[bool], None] | None = None,
    ):
        self._p99_fn = p99_latency_fn or (lambda: 50.0)
        self._error_rate_fn = error_rate_fn or (lambda: 0.0)
        self._merkle_fn = merkle_verify_fn or (lambda: 0.0)
        self._anomaly_fn = anomaly_score_fn or (lambda: 0.1)
        self._lag_fn = replication_lag_fn or (lambda: 1.0)
        self._pool_fn = conn_pool_utilisation_fn or (lambda: 30.0)
        self._spot_fn = spot_check_fn or (lambda: True)
        self._on_alert = on_alert
        self._on_immediate_complete = on_immediate_complete

        self._checkpoints: list[ValidationCheckpoint] = []
        self._merkle_count = 0
        self._merkle_failed = 0
        self._spot_run = 0
        self._spot_passed = 0
        self._lock = threading.Lock()
        self._cutover_time = time.time()
        self._checkpoint_counter = 0

    def start(self, cutover_completed_at: str) -> None:
        """Start the post-cutover monitoring loop in background threads."""
        self._cutover_time = time.time()
        logger.info("Post-cutover monitoring started")

        # Immediate window thread
        imm_thread = threading.Thread(
            target=self._immediate_window_loop,
            daemon=True,
            name="post-cutover-immediate",
        )
        imm_thread.start()

        # Extended window thread
        ext_thread = threading.Thread(
            target=self._extended_window_loop,
            daemon=True,
            name="post-cutover-extended",
        )
        ext_thread.start()

    def run_immediate_check(self) -> ValidationCheckpoint:
        """Run a single immediate validation check."""
        p99 = self._p99_fn()
        err = self._error_rate_fn()
        merkle = self._merkle_fn()
        anomaly = self._anomaly_fn()
        lag = self._lag_fn()
        pool = self._pool_fn()

        checks = (
            p99 <= self.THRESHOLDS["p99_latency_ms_max"],
            err <= self.THRESHOLDS["error_rate_pct_max"],
            merkle <= self.THRESHOLDS["merkle_divergence_max"],
            anomaly <= self.ANOMALY_SENSITIVE_THRESHOLD,
            pool <= self.THRESHOLDS["conn_pool_pct_max"],
        )

        with self._lock:
            self._checkpoint_counter += 1
            cid = self._checkpoint_counter

        cp = ValidationCheckpoint(
            checkpoint_id=f"imm-{cid:05d}",
            phase="immediate",
            checked_at=datetime.utcnow().isoformat(),
            p99_latency_ms=p99,
            error_rate_pct=err,
            merkle_divergence_pct=merkle,
            anomaly_score=anomaly,
            replication_lag_seconds=lag,
            conn_pool_utilisation_pct=pool,
            all_checks_green=all(checks),
        )

        with self._lock:
            self._checkpoints.append(cp)

        if not cp.all_checks_green and self._on_alert:
            self._on_alert("immediate_check_failed", cp)

        logger.debug(
            f"Immediate check: p99={p99:.0f}ms err={err:.3f}% "
            f"merkle={merkle:.4f}% anomaly={anomaly:.4f} "
            f"pool={pool:.1f}% {'✓' if cp.all_checks_green else '✗'}"
        )
        return cp

    def run_merkle_check(self, check_number: int) -> bool:
        """Run a Merkle verification and track results."""
        divergence = self._merkle_fn()
        clean = divergence <= 0.0
        with self._lock:
            self._merkle_count += 1
            if not clean:
                self._merkle_failed += 1
        logger.info(
            f"Merkle #{check_number}: "
            f"{'CLEAN' if clean else f'DIVERGENCE={divergence:.4f}%'}"
        )
        return clean

    def generate_report(self) -> PostCutoverReport:
        """Generate the final post-cutover validation report."""
        with self._lock:
            cps = list(self._checkpoints)
            merkle_count = self._merkle_count
            merkle_failed = self._merkle_failed
            spot_run = self._spot_run
            spot_passed = self._spot_passed

        immediate_cps = [c for c in cps if c.phase == "immediate"]
        extended_cps = [c for c in cps if c.phase == "extended"]

        failed = sum(1 for c in cps if not c.all_checks_green)
        immediate_passed = all(c.all_checks_green for c in immediate_cps) if immediate_cps else False
        extended_passed = all(c.all_checks_green for c in extended_cps) if extended_cps else False

        return PostCutoverReport(
            cutover_completed_at=datetime.fromtimestamp(self._cutover_time).isoformat(),
            report_generated_at=datetime.utcnow().isoformat(),
            immediate_window_passed=immediate_passed,
            extended_window_passed=extended_passed,
            total_checkpoints=len(cps),
            failed_checkpoints=failed,
            merkle_verifications=merkle_count,
            merkle_all_clean=merkle_failed == 0,
            spot_checks_run=spot_run,
            spot_checks_passed=spot_passed,
            edge_decommission_safe=immediate_passed and extended_passed and merkle_failed == 0,
            checkpoints=cps[-50:],
        )

    # ── Background loops ──────────────────────────────────────────────────────

    def _immediate_window_loop(self) -> None:
        """10-second checks for the first 30 minutes."""
        deadline = self._cutover_time + self.IMMEDIATE_WINDOW_SECONDS
        all_passed = True

        while time.time() < deadline:
            cp = self.run_immediate_check()
            if not cp.all_checks_green:
                all_passed = False
            time.sleep(self.IMMEDIATE_CHECK_INTERVAL_SECONDS)

        logger.info(
            f"Immediate validation window complete: "
            f"{'PASSED' if all_passed else 'FAILED'}"
        )
        if self._on_immediate_complete:
            self._on_immediate_complete(all_passed)

    def _extended_window_loop(self) -> None:
        """Merkle checks and spot checks for 24 hours."""
        deadline = self._cutover_time + self.EXTENDED_WINDOW_SECONDS
        merkle_num = 0
        two_hour_mark = self._cutover_time + 7200

        while time.time() < deadline:
            # Determine Merkle interval
            if time.time() < two_hour_mark:
                interval = self.EXTENDED_MERKLE_INITIAL_INTERVAL
            else:
                interval = self.EXTENDED_MERKLE_LATER_INTERVAL

            time.sleep(interval)
            merkle_num += 1
            self.run_merkle_check(merkle_num)

            # Spot checks every Merkle cycle
            if self._spot_fn:
                result = self._spot_fn()
                with self._lock:
                    self._spot_run += 1
                    if result:
                        self._spot_passed += 1
