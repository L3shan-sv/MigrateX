"""
observability/metrics.py
────────────────────────
Prometheus metrics exporter for Phase 2 infrastructure.

Publishes all MigrateX metrics to /metrics endpoint on port 8000.
Grafana dashboards consume these via the Prometheus data source.

Metric categories:
  migratex_cdc_*         — CDC pipeline health (lag, throughput, connector state)
  migratex_sink_*        — Sink consumer metrics (processed, deduped, failed, dlq)
  migratex_snapshot_*    — Snapshot coordinator progress
  migratex_lease_*       — etcd lease fencing state
  migratex_replication_* — Replication lag (consumed by PONR engine)

All metrics use standard Prometheus naming conventions:
  - Counters always end in _total
  - Gauges describe current state
  - Histograms track latency distributions
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

try:
    from prometheus_client import (
        Counter,
        Gauge,
        Histogram,
        Info,
        start_http_server,
        REGISTRY,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger.warning("prometheus_client not available — metrics will be no-ops")


class MigrateXMetrics:
    """
    Central metrics registry for Phase 2.
    All components publish through this singleton.
    """

    def __init__(self, port: int = 8000):
        self._port = port
        self._started = False
        self._lock = threading.Lock()

        if not PROMETHEUS_AVAILABLE:
            return

        # ── CDC metrics ───────────────────────────────────────────────────────
        self.cdc_connector_state = Gauge(
            "migratex_cdc_connector_state",
            "Debezium connector state (1=RUNNING, 0=not running)",
            ["connector_name"],
        )
        self.cdc_lag_seconds = Gauge(
            "migratex_cdc_lag_seconds",
            "Current CDC replication lag in seconds",
        )
        self.cdc_events_total = Counter(
            "migratex_cdc_events_total",
            "Total CDC events emitted by Debezium",
            ["operation"],  # insert, update, delete, read
        )

        # ── Sink consumer metrics ─────────────────────────────────────────────
        self.sink_events_processed_total = Counter(
            "migratex_sink_events_processed_total",
            "Total events successfully applied to target DB",
        )
        self.sink_events_deduplicated_total = Counter(
            "migratex_sink_events_deduplicated_total",
            "Total events discarded by GTID deduplication",
        )
        self.sink_events_failed_total = Counter(
            "migratex_sink_events_failed_total",
            "Total events that failed to apply after retries",
        )
        self.sink_events_dlq_total = Counter(
            "migratex_sink_events_dlq_total",
            "Total events sent to dead letter queue",
        )
        self.sink_lag_seconds = Gauge(
            "migratex_sink_lag_seconds",
            "Current Kafka consumer group lag in seconds",
        )
        self.sink_apply_latency = Histogram(
            "migratex_sink_apply_latency_seconds",
            "Time to apply a single change event to target DB",
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
        )

        # ── Snapshot metrics ──────────────────────────────────────────────────
        self.snapshot_progress_pct = Gauge(
            "migratex_snapshot_progress_pct",
            "Snapshot completion percentage (0-100)",
        )
        self.snapshot_chunks_completed_total = Counter(
            "migratex_snapshot_chunks_completed_total",
            "Total snapshot chunks successfully transferred",
        )
        self.snapshot_chunks_failed_total = Counter(
            "migratex_snapshot_chunks_failed_total",
            "Total snapshot chunks that failed after retries",
        )
        self.snapshot_throughput_mbps = Gauge(
            "migratex_snapshot_throughput_mbps",
            "Current snapshot throughput in MB/s",
        )
        self.snapshot_rows_transferred_total = Counter(
            "migratex_snapshot_rows_transferred_total",
            "Total rows transferred during snapshot",
        )
        self.snapshot_deadman_triggered = Gauge(
            "migratex_snapshot_deadman_triggered",
            "1 if dead-man's switch has fired (progress stalled), 0 otherwise",
        )

        # ── Lease fencing metrics ─────────────────────────────────────────────
        self.lease_epoch = Gauge(
            "migratex_lease_epoch",
            "Current fencing token epoch",
        )
        self.lease_healthy = Gauge(
            "migratex_lease_healthy",
            "1 if etcd lease is healthy, 0 otherwise",
        )
        self.lease_authority = Gauge(
            "migratex_lease_authority",
            "Current write authority (0=none, 1=edge, 2=cloud)",
        )
        self.fencing_violations_total = Counter(
            "migratex_fencing_violations_total",
            "Total write attempts rejected due to stale fencing epoch",
        )

        # ── Replication metrics (fed to PONR engine) ──────────────────────────
        self.replication_lag_seconds = Gauge(
            "migratex_replication_lag_seconds",
            "MySQL replication lag on target (seconds behind source)",
        )
        self.bytes_transferred_total = Counter(
            "migratex_bytes_transferred_total",
            "Total bytes transferred from source to target",
        )
        self.migration_progress_pct = Gauge(
            "migratex_migration_progress_pct",
            "Overall migration progress percentage (0-100)",
        )

        # ── Alert state ───────────────────────────────────────────────────────
        self.alert_state = Gauge(
            "migratex_alert_state",
            "Current alert state (0=normal, 1=caution, 2=critical)",
            ["alert_name"],
        )

        # ── Build info ────────────────────────────────────────────────────────
        self.build_info = Info(
            "migratex",
            "MigrateX build information",
        )
        self.build_info.info({
            "version": "1.0.0",
            "phase": "2",
            "component": "infrastructure",
        })

    def start_server(self) -> None:
        """Start Prometheus HTTP server on configured port."""
        if not PROMETHEUS_AVAILABLE:
            logger.warning("Prometheus not available — skipping metrics server")
            return
        with self._lock:
            if self._started:
                return
            start_http_server(self._port)
            self._started = True
            logger.info(f"Prometheus metrics server started on port {self._port}")

    # ── Update helpers ────────────────────────────────────────────────────────

    def update_cdc_lag(self, lag_seconds: float) -> None:
        if PROMETHEUS_AVAILABLE:
            self.cdc_lag_seconds.set(lag_seconds)
            self.sink_lag_seconds.set(lag_seconds)
            self.replication_lag_seconds.set(lag_seconds)

    def update_sink_metrics(self, processed: int, deduped: int, failed: int, dlq: int) -> None:
        if PROMETHEUS_AVAILABLE:
            self.sink_events_processed_total._value.set(processed)
            self.sink_events_deduplicated_total._value.set(deduped)
            self.sink_events_failed_total._value.set(failed)
            self.sink_events_dlq_total._value.set(dlq)

    def update_snapshot(self, progress_pct: float, throughput_mbps: float, deadman: bool) -> None:
        if PROMETHEUS_AVAILABLE:
            self.snapshot_progress_pct.set(progress_pct)
            self.snapshot_throughput_mbps.set(throughput_mbps)
            self.snapshot_deadman_triggered.set(1 if deadman else 0)

    def update_lease(self, epoch: int, healthy: bool, authority_str: str) -> None:
        if PROMETHEUS_AVAILABLE:
            self.lease_epoch.set(epoch)
            self.lease_healthy.set(1 if healthy else 0)
            authority_map = {"none": 0, "edge": 1, "cloud": 2}
            self.lease_authority.set(authority_map.get(authority_str, 0))

    def record_fencing_violation(self) -> None:
        if PROMETHEUS_AVAILABLE:
            self.fencing_violations_total.inc()

    def set_alert_state(self, alert_name: str, state: int) -> None:
        """state: 0=normal, 1=caution, 2=critical"""
        if PROMETHEUS_AVAILABLE:
            self.alert_state.labels(alert_name=alert_name).set(state)


# ── Singleton ─────────────────────────────────────────────────────────────────

_metrics_instance: MigrateXMetrics | None = None


def get_metrics(port: int = 8000) -> MigrateXMetrics:
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = MigrateXMetrics(port=port)
    return _metrics_instance
