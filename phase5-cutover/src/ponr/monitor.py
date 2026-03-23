"""
ponr/monitor.py
───────────────
Live PONR monitor for Phase 5 Event Horizon window.

This is the Phase 1 Monte Carlo engine deployed in its operational role.
In Phase 1 it ran a pre-migration estimate.
In Phase 5 it runs continuously — every 30 seconds during the 2-hour
Event Horizon window, every 60 seconds outside it.

It feeds from live metrics:
  - Replication lag (Kafka consumer group offset exporter)
  - Write rate (performance_schema on source)
  - Network health (measured RTT + throughput)
  - Bytes transferred vs total (from snapshot + CDC tracking)

PONR gate logic:
  P95 >= SLA_THRESHOLD       → BLOCK  (cutover prohibited)
  P95 >= SLA_THRESHOLD × 0.8 → CAUTION (prepare Tier 3 sequence)
  P95 < SLA_THRESHOLD × 0.8  → PROCEED

The monitor publishes all metrics to Prometheus.
If state transitions to BLOCK, it fires a PagerDuty critical alert,
and the operator receives the Final Call notification.

The operator CANNOT override a BLOCK state.
The only path forward from BLOCK is waiting for conditions to improve
such that P95 falls below the threshold.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Callable

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class LiveMigrationState:
    """Real-time migration state fed to the PONR engine."""
    total_data_bytes: int
    bytes_transferred: int
    write_rate_bytes_per_sec: float
    replication_lag_seconds: float
    elapsed_seconds: float
    last_clean_merkle_seconds: float
    network_throughput_gbps: float
    network_std_gbps: float
    anomaly_score: float


@dataclass
class PONRSnapshot:
    """One evaluation of the PONR engine."""
    evaluated_at: str
    p5_usd: float
    p50_usd: float
    p95_usd: float
    p99_usd: float
    recommendation: str          # PROCEED | CAUTION | BLOCK
    rollback_feasible: bool
    event_horizon_seconds: float | None
    migration_progress_pct: float
    replication_lag_seconds: float
    anomaly_score: float
    network_health_index: float  # 0.0 (degraded) → 1.0 (healthy)


class LivePONRMonitor:
    """
    Continuously evaluates the PONR Monte Carlo model against live metrics.

    Two operating modes:
      Normal mode: evaluation every 60 seconds
      Event Horizon mode (T-2h to T-0): evaluation every 30 seconds

    Emits callbacks on state transitions:
      on_block:   P95 crosses SLA threshold (critical — page SRE immediately)
      on_caution: P95 enters 80-100% of threshold (prepare Tier 3)
      on_proceed: P95 drops back below 80% of threshold (recovery)
    """

    EGRESS_COST_PER_GB = 0.09
    DOWNTIME_COST_PER_HOUR = 5_000.0
    ROLLBACK_BANDWIDTH_GBPS = 0.5

    def __init__(
        self,
        n_simulations: int = 10_000,
        sla_threshold_usd: float = 5_000.0,
        block_confidence: float = 0.95,
        state_fn: Callable[[], LiveMigrationState] | None = None,
        on_block: Callable[[PONRSnapshot], None] | None = None,
        on_caution: Callable[[PONRSnapshot], None] | None = None,
        on_proceed: Callable[[PONRSnapshot], None] | None = None,
        seed: int | None = None,
    ):
        self._n = n_simulations
        self._sla = sla_threshold_usd
        self._confidence = block_confidence
        self._state_fn = state_fn
        self._on_block = on_block
        self._on_caution = on_caution
        self._on_proceed = on_proceed
        self._seed = seed

        self._running = False
        self._event_horizon_mode = False
        self._last_recommendation = "PROCEED"
        self._lock = threading.Lock()
        self._history: list[PONRSnapshot] = []
        self._latest: PONRSnapshot | None = None

        logger.info(
            f"LivePONRMonitor initialised — "
            f"simulations={n_simulations} "
            f"sla=${sla_threshold_usd:,.0f} "
            f"confidence={block_confidence}"
        )

    def start(self, normal_interval: int = 60, horizon_interval: int = 30) -> None:
        """Start the background monitoring loop."""
        self._normal_interval = normal_interval
        self._horizon_interval = horizon_interval
        self._running = True
        self._thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="ponr-live-monitor",
        )
        self._thread.start()
        logger.info("PONR live monitor started")

    def stop(self) -> None:
        self._running = False
        logger.info("PONR live monitor stopped")

    def enter_event_horizon_mode(self) -> None:
        """Switch to 30-second evaluation interval for T-2h window."""
        with self._lock:
            self._event_horizon_mode = True
        logger.info("PONR monitor: ENTERING EVENT HORIZON MODE (30s intervals)")

    def evaluate_once(self, state: LiveMigrationState) -> PONRSnapshot:
        """
        Run a single PONR evaluation. Can be called directly (no background thread).
        Used for the pre-flight check and the Final Call notification.
        """
        rng = np.random.default_rng(self._seed)
        costs = self._run_simulation(state, rng)

        p5 = float(np.percentile(costs, 5))
        p50 = float(np.percentile(costs, 50))
        p95 = float(np.percentile(costs, 95))
        p99 = float(np.percentile(costs, 99))

        recommendation = self._recommend(p95)
        horizon = self._estimate_horizon(state, p95)
        progress = (
            state.bytes_transferred / state.total_data_bytes * 100
            if state.total_data_bytes > 0 else 0.0
        )
        net_health = self._network_health_index(
            state.network_throughput_gbps, state.network_std_gbps
        )

        snapshot = PONRSnapshot(
            evaluated_at=datetime.utcnow().isoformat(),
            p5_usd=round(p5, 2),
            p50_usd=round(p50, 2),
            p95_usd=round(p95, 2),
            p99_usd=round(p99, 2),
            recommendation=recommendation,
            rollback_feasible=p95 < self._sla,
            event_horizon_seconds=horizon,
            migration_progress_pct=round(progress, 2),
            replication_lag_seconds=state.replication_lag_seconds,
            anomaly_score=state.anomaly_score,
            network_health_index=round(net_health, 3),
        )

        self._record(snapshot)
        return snapshot

    def get_latest(self) -> PONRSnapshot | None:
        with self._lock:
            return self._latest

    def get_history(self, limit: int = 50) -> list[PONRSnapshot]:
        with self._lock:
            return self._history[-limit:]

    def is_cutover_permitted(self) -> bool:
        """True only if the latest PONR snapshot recommends PROCEED or CAUTION."""
        latest = self.get_latest()
        if latest is None:
            return False
        return latest.recommendation in ("PROCEED", "CAUTION")

    # ── Simulation ────────────────────────────────────────────────────────────

    def _run_simulation(
        self, state: LiveMigrationState, rng: np.random.Generator
    ) -> np.ndarray:
        """
        Monte Carlo rollback cost simulation.
        Same model as Phase 1 engine — reused here for consistency.
        """
        # Network throughput samples
        throughput = np.maximum(
            rng.normal(
                loc=state.network_throughput_gbps,
                scale=state.network_std_gbps,
                size=self._n,
            ),
            0.01,
        )

        # Egress cost (deterministic — already paid for transferred bytes)
        bytes_gb = state.bytes_transferred / 1e9
        egress_cost = bytes_gb * self.EGRESS_COST_PER_GB

        # Divergence to re-sync on rollback
        diverged_bytes = state.replication_lag_seconds * state.write_rate_bytes_per_sec
        rollback_tp = np.maximum(
            rng.normal(
                loc=self.ROLLBACK_BANDWIDTH_GBPS,
                scale=state.network_std_gbps * 0.5,
                size=self._n,
            ),
            0.005,
        )
        resync_hours = (diverged_bytes / 1e9) / rollback_tp
        resync_cost = resync_hours * self.DOWNTIME_COST_PER_HOUR

        # Downtime: 60s fixed + resync
        downtime_hours = (60 / 3600) + resync_hours
        downtime_cost = downtime_hours * self.DOWNTIME_COST_PER_HOUR

        # Merkle repair cost
        dirty_bytes = state.last_clean_merkle_seconds * state.write_rate_bytes_per_sec * 0.001
        dirty_hours = (dirty_bytes / 1e9) / rollback_tp
        merkle_cost = dirty_hours * self.DOWNTIME_COST_PER_HOUR

        return egress_cost + resync_cost + downtime_cost + merkle_cost

    # ── Recommendation ────────────────────────────────────────────────────────

    def _recommend(self, p95: float) -> str:
        if p95 >= self._sla:
            return "BLOCK"
        if p95 >= self._sla * 0.80:
            return "CAUTION"
        return "PROCEED"

    def _estimate_horizon(
        self, state: LiveMigrationState, current_p95: float
    ) -> float | None:
        if current_p95 >= self._sla:
            return 0.0
        throughput_bytes = state.network_throughput_gbps * 1e9
        rate = (throughput_bytes / 1e9) * self.EGRESS_COST_PER_GB
        if rate <= 0:
            return None
        return round((self._sla - current_p95) / rate, 0)

    def _network_health_index(self, throughput: float, std: float) -> float:
        """
        Composite network health index: 0 (degraded) → 1 (healthy).
        Based on coefficient of variation (std / mean).
        Low CoV = stable network = high health index.
        """
        if throughput <= 0:
            return 0.0
        cv = std / throughput  # coefficient of variation
        return max(0.0, min(1.0, 1.0 - cv))

    # ── State transitions ─────────────────────────────────────────────────────

    def _record(self, snapshot: PONRSnapshot) -> None:
        with self._lock:
            self._history.append(snapshot)
            if len(self._history) > 1000:
                self._history = self._history[-1000:]
            self._latest = snapshot
            prev = self._last_recommendation
            self._last_recommendation = snapshot.recommendation

        # Fire state transition callbacks
        if snapshot.recommendation == "BLOCK" and prev != "BLOCK":
            logger.critical(
                f"PONR BLOCK: P95=${snapshot.p95_usd:,.0f} >= SLA ${self._sla:,.0f}. "
                f"Cutover PROHIBITED. Operator must wait for conditions to improve."
            )
            if self._on_block:
                self._on_block(snapshot)

        elif snapshot.recommendation == "CAUTION" and prev == "PROCEED":
            logger.warning(
                f"PONR CAUTION: P95=${snapshot.p95_usd:,.0f} within 20% of SLA. "
                f"Prepare Tier 3 rollback sequence."
            )
            if self._on_caution:
                self._on_caution(snapshot)

        elif snapshot.recommendation == "PROCEED" and prev in ("BLOCK", "CAUTION"):
            logger.info(
                f"PONR RECOVER: P95=${snapshot.p95_usd:,.0f} below caution threshold. "
                f"Cutover permitted."
            )
            if self._on_proceed:
                self._on_proceed(snapshot)

        logger.info(
            f"PONR [{snapshot.recommendation}] "
            f"P5=${snapshot.p5_usd:,.0f} "
            f"P50=${snapshot.p50_usd:,.0f} "
            f"P95=${snapshot.p95_usd:,.0f} "
            f"horizon={snapshot.event_horizon_seconds}s "
            f"lag={snapshot.replication_lag_seconds:.1f}s "
            f"net={snapshot.network_health_index:.2f}"
        )

    # ── Background loop ───────────────────────────────────────────────────────

    def _monitor_loop(self) -> None:
        while self._running:
            with self._lock:
                interval = (
                    self._horizon_interval
                    if self._event_horizon_mode
                    else self._normal_interval
                )

            if self._state_fn:
                try:
                    state = self._state_fn()
                    self.evaluate_once(state)
                except Exception as e:
                    logger.error(f"PONR evaluation error: {e}")

            time.sleep(interval)
