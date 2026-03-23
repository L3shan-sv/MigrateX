"""
finops/arbitrator.py
────────────────────
FinOps arbitrator — cost optimization bounded by migration deadline.

Hard rule from the fallback plan:
  The migration deadline SLA is a HARD CEILING.
  Cost optimization is best-effort within that window.
  When the deadline is within range, cost optimization is suspended.
  Critical writes are NEVER buffered — they flow unconditionally.

Write classification:
  CRITICAL — booking state, payment state, user account state
             → Always flows immediately. Never buffered.
  NON_CRITICAL — analytics events, audit log writes, soft-deletes
                → Eligible for WAL buffering during price spikes.

FinOps triggers:
  When current egress price > 1.5× 30-day baseline:
    → Buffer non-critical writes to local WAL
  When current egress price normalizes:
    → Drain WAL (max age: 2 hours)
  When WAL age > 2 hours (undrained):
    → Class C alert fires
  When cutover window is within N hours:
    → Cost optimization suspended, all writes flow unconditionally

Cost tracking:
  Real-time egress meter: bytes/min × current price/GB
  Cumulative cost vs Phase 1 P50 estimate
  Projection to total cost at current rate
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class WriteClass(Enum):
    CRITICAL = "critical"           # Never buffered
    NON_CRITICAL = "non_critical"   # Bufferable during price spikes


class FinOpsDecision(Enum):
    FLOW_IMMEDIATELY = "flow_immediately"
    BUFFER_TO_WAL = "buffer_to_wal"
    FLUSH_BUFFER = "flush_buffer"
    DEADLINE_OVERRIDE = "deadline_override"  # cost opt suspended


@dataclass
class WALEntry:
    """A buffered non-critical write waiting for cost normalization."""
    entry_id: str
    table_name: str
    operation: str
    row_data: dict
    buffered_at: str
    bytes_estimate: int


@dataclass
class EgressMeter:
    current_price_per_gb: float
    baseline_price_per_gb: float
    spike_multiplier: float         # current / baseline
    bytes_transferred_today: int
    cost_today_usd: float
    projected_total_usd: float
    phase1_p50_estimate_usd: float
    is_spike: bool


@dataclass
class FinOpsStatus:
    cost_optimization_active: bool
    deadline_override_active: bool
    wal_buffer_size: int
    wal_oldest_entry_age_hours: float
    current_egress_price: float
    baseline_egress_price: float
    spike_detected: bool
    total_cost_usd: float
    projected_total_usd: float
    savings_attributed_usd: float


# ── Write classifier ──────────────────────────────────────────────────────────

CRITICAL_TABLES = frozenset({
    "bookings", "booking_payments", "booking_status",
    "users", "user_accounts", "user_payments",
    "payments", "payment_transactions", "payment_methods",
    "reservations", "holds",
})

NON_CRITICAL_TABLES = frozenset({
    "analytics_events", "page_views", "search_logs",
    "audit_logs", "activity_logs",
    "deleted_records", "soft_deletes",
    "email_queue", "notification_queue",
})


def classify_write(table_name: str) -> WriteClass:
    """
    Classify a write operation as critical or non-critical.
    When in doubt, classify as CRITICAL (safe default).
    """
    table_lower = table_name.lower()
    if table_lower in CRITICAL_TABLES:
        return WriteClass.CRITICAL
    if table_lower in NON_CRITICAL_TABLES:
        return WriteClass.NON_CRITICAL
    # Unknown table → critical by default
    return WriteClass.CRITICAL


# ── FinOps Arbitrator ─────────────────────────────────────────────────────────

class FinOpsArbitrator:
    """
    Real-time cost arbitrator for Phase 4 egress optimization.

    Activated at the start of Phase 4 and runs continuously through Phase 5.
    Makes per-write decisions: flow immediately or buffer to WAL.
    Monitors cumulative cost vs Phase 1 P50 estimate.
    """

    BYTES_PER_GB = 1_000_000_000
    WAL_MAX_AGE_HOURS = 2.0
    WAL_DRAIN_CHECK_INTERVAL_SECONDS = 60
    DEADLINE_BUFFER_HOURS = 24   # suspend cost opt when deadline < 24h away

    def __init__(
        self,
        baseline_price_per_gb: float = 0.09,
        spike_multiplier_threshold: float = 1.5,
        phase1_p50_estimate_usd: float = 500.0,
        migration_deadline_hours: int = 240,
        on_wal_age_alert: Callable[[float], None] | None = None,
        on_cost_overrun: Callable[[float], None] | None = None,
        price_fn: Callable[[], float] | None = None,
    ):
        self._baseline = baseline_price_per_gb
        self._spike_threshold = spike_multiplier_threshold
        self._p50_estimate = phase1_p50_estimate_usd
        self._deadline_hours = migration_deadline_hours
        self._on_wal_age_alert = on_wal_age_alert
        self._on_cost_overrun = on_cost_overrun
        self._price_fn = price_fn or (lambda: baseline_price_per_gb)

        self._wal_buffer: list[WALEntry] = []
        self._lock = threading.Lock()
        self._total_bytes = 0
        self._total_cost_usd = 0.0
        self._savings_usd = 0.0
        self._started_at: float = time.time()
        self._migration_start_time: float = time.time()
        self._entry_counter = 0

        # Start WAL drain monitor
        self._drain_thread = threading.Thread(
            target=self._drain_monitor,
            daemon=True,
            name="finops-drain-monitor",
        )
        self._drain_thread.start()
        logger.info(
            f"FinOpsArbitrator activated — "
            f"baseline=${baseline_price_per_gb}/GB "
            f"spike_threshold={spike_multiplier_threshold}x "
            f"p50_estimate=${phase1_p50_estimate_usd:,.0f}"
        )

    def decide(
        self,
        table_name: str,
        operation: str,
        row_data: dict,
        bytes_estimate: int = 1024,
    ) -> FinOpsDecision:
        """
        Make a per-write FinOps decision.
        Returns the decision: flow immediately or buffer to WAL.

        Critical writes always return FLOW_IMMEDIATELY.
        Non-critical writes may return BUFFER_TO_WAL during price spikes.
        All writes return DEADLINE_OVERRIDE near the migration deadline.
        """
        write_class = classify_write(table_name)

        # Check deadline proximity
        if self._deadline_approaching():
            logger.debug(f"DEADLINE_OVERRIDE: {table_name} — deadline within {self.DEADLINE_BUFFER_HOURS}h")
            self._track_bytes(bytes_estimate)
            return FinOpsDecision.DEADLINE_OVERRIDE

        # Critical writes always flow
        if write_class == WriteClass.CRITICAL:
            self._track_bytes(bytes_estimate)
            return FinOpsDecision.FLOW_IMMEDIATELY

        # Non-critical: check for price spike
        current_price = self._price_fn()
        spike_multiplier = current_price / max(self._baseline, 0.001)

        if spike_multiplier >= self._spike_threshold:
            self._buffer_write(table_name, operation, row_data, bytes_estimate)
            savings = bytes_estimate / self.BYTES_PER_GB * (current_price - self._baseline)
            with self._lock:
                self._savings_usd += savings
            logger.debug(
                f"BUFFERED: {table_name} — price={current_price:.4f}/GB "
                f"({spike_multiplier:.2f}x baseline)"
            )
            return FinOpsDecision.BUFFER_TO_WAL

        # Price normal — flow immediately
        self._track_bytes(bytes_estimate)
        return FinOpsDecision.FLOW_IMMEDIATELY

    def flush_wal(self, apply_fn: Callable[[list[WALEntry]], int] | None = None) -> int:
        """
        Drain the WAL buffer. Returns number of entries flushed.
        Called when: price normalizes, deadline approaches, or max age exceeded.
        """
        with self._lock:
            if not self._wal_buffer:
                return 0
            entries = list(self._wal_buffer)
            self._wal_buffer.clear()

        if apply_fn:
            flushed = apply_fn(entries)
        else:
            flushed = len(entries)

        for entry in entries:
            self._track_bytes(entry.bytes_estimate)

        logger.info(f"WAL flushed: {flushed} entries")
        return flushed

    def get_meter(self) -> EgressMeter:
        current_price = self._price_fn()
        with self._lock:
            total_bytes = self._total_bytes
            total_cost = self._total_cost_usd

        elapsed_hours = (time.time() - self._started_at) / 3600
        if elapsed_hours > 0:
            bytes_per_hour = total_bytes / elapsed_hours
            projected_bytes = bytes_per_hour * self._deadline_hours
            projected_cost = (projected_bytes / self.BYTES_PER_GB) * current_price
        else:
            projected_cost = 0.0

        return EgressMeter(
            current_price_per_gb=current_price,
            baseline_price_per_gb=self._baseline,
            spike_multiplier=current_price / max(self._baseline, 0.001),
            bytes_transferred_today=total_bytes,
            cost_today_usd=total_cost,
            projected_total_usd=round(projected_cost, 2),
            phase1_p50_estimate_usd=self._p50_estimate,
            is_spike=current_price >= self._baseline * self._spike_threshold,
        )

    def get_status(self) -> FinOpsStatus:
        meter = self.get_meter()
        with self._lock:
            wal_size = len(self._wal_buffer)
            oldest_age = self._oldest_wal_age_hours()
            savings = self._savings_usd
            total_cost = self._total_cost_usd

        return FinOpsStatus(
            cost_optimization_active=not self._deadline_approaching(),
            deadline_override_active=self._deadline_approaching(),
            wal_buffer_size=wal_size,
            wal_oldest_entry_age_hours=oldest_age,
            current_egress_price=meter.current_price_per_gb,
            baseline_egress_price=self._baseline,
            spike_detected=meter.is_spike,
            total_cost_usd=total_cost,
            projected_total_usd=meter.projected_total_usd,
            savings_attributed_usd=savings,
        )

    # ── Internal ──────────────────────────────────────────────────────────────

    def _buffer_write(
        self, table: str, operation: str, row_data: dict, bytes_estimate: int
    ) -> None:
        with self._lock:
            self._entry_counter += 1
            entry = WALEntry(
                entry_id=f"wal-{self._entry_counter:08d}",
                table_name=table,
                operation=operation,
                row_data=row_data,
                buffered_at=datetime.utcnow().isoformat(),
                bytes_estimate=bytes_estimate,
            )
            self._wal_buffer.append(entry)

    def _track_bytes(self, bytes_estimate: int) -> None:
        current_price = self._price_fn()
        cost = (bytes_estimate / self.BYTES_PER_GB) * current_price
        with self._lock:
            self._total_bytes += bytes_estimate
            self._total_cost_usd += cost

        if self._total_cost_usd > self._p50_estimate * 1.5:
            logger.warning(
                f"Cost overrun: ${self._total_cost_usd:,.2f} > "
                f"1.5× P50 estimate ${self._p50_estimate:,.2f}"
            )
            if self._on_cost_overrun:
                self._on_cost_overrun(self._total_cost_usd)

    def _deadline_approaching(self) -> bool:
        elapsed_hours = (time.time() - self._migration_start_time) / 3600
        remaining_hours = self._deadline_hours - elapsed_hours
        return remaining_hours <= self.DEADLINE_BUFFER_HOURS

    def _oldest_wal_age_hours(self) -> float:
        if not self._wal_buffer:
            return 0.0
        oldest = self._wal_buffer[0].buffered_at
        try:
            ts = datetime.fromisoformat(oldest)
            age = (datetime.utcnow() - ts).total_seconds() / 3600
            return round(age, 3)
        except Exception:
            return 0.0

    def _drain_monitor(self) -> None:
        """
        Background thread: monitors WAL age and triggers drain
        when entries are approaching the 2-hour max age.
        Fires a Class C alert if WAL is not drained within 2 hours.
        """
        while True:
            time.sleep(self.WAL_DRAIN_CHECK_INTERVAL_SECONDS)
            with self._lock:
                age = self._oldest_wal_age_hours()

            if age > self.WAL_MAX_AGE_HOURS:
                logger.warning(
                    f"WAL age alert: oldest entry is {age:.2f}h old "
                    f"(max={self.WAL_MAX_AGE_HOURS}h) — Class C alert firing"
                )
                if self._on_wal_age_alert:
                    self._on_wal_age_alert(age)
                # Auto-flush on age breach
                self.flush_wal()

            elif age > self.WAL_MAX_AGE_HOURS * 0.5:
                # Price may have normalized — check and drain
                current_price = self._price_fn()
                spike = current_price / max(self._baseline, 0.001)
                if spike < self._spike_threshold:
                    self.flush_wal()
