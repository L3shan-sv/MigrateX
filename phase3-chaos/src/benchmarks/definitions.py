"""
benchmarks/definitions.py
──────────────────────────
The 6 performance benchmarks that must pass during 5x traffic replay.
Each benchmark maps a metric to a threshold. All 6 must pass to
contribute the full 30 benchmark points to the resilience score.

Benchmarks are evaluated at the END of the 48-hour replay window,
not during. Peak values are tracked throughout.
"""

from __future__ import annotations

from src.resilience.scorer import BenchmarkResult
from src.replay.engine import BenchmarkSnapshot


BENCHMARKS = [
    {
        "id": "B-001",
        "name": "Peak consumer lag at 5x load",
        "metric": "peak_consumer_lag_seconds",
        "threshold": 120.0,
        "unit": "seconds",
        "comparison": "lte",    # actual <= threshold
        "description": "Consumer lag must stay below 120s even at 5x production load.",
    },
    {
        "id": "B-002",
        "name": "Target P99 INSERT latency at 5x",
        "metric": "p99_insert_latency_ms",
        "threshold": 200.0,
        "unit": "ms",
        "comparison": "lte",
        "description": "P99 INSERT latency on cloud target must stay below 200ms at 5x load.",
    },
    {
        "id": "B-003",
        "name": "Target P99 SELECT latency at 5x",
        "metric": "p99_select_latency_ms",
        "threshold": 150.0,
        "unit": "ms",
        "comparison": "lte",
        "description": "P99 SELECT latency on cloud target must stay below 150ms at 5x load.",
    },
    {
        "id": "B-004",
        "name": "Merkle divergence during replay",
        "metric": "max_merkle_divergence_pct",
        "threshold": 0.001,
        "unit": "%",
        "comparison": "lte",
        "description": "Max Merkle divergence in any 10-minute window must be < 0.001%.",
    },
    {
        "id": "B-005",
        "name": "PONR model stability",
        "metric": "ponr_stability_pct",
        "threshold": 20.0,
        "unit": "%",
        "comparison": "lte",
        "description": "Live P95 rollback cost must stay within 20% of Phase 1 estimate.",
    },
    {
        "id": "B-006",
        "name": "etcd lease renewal success rate",
        "metric": "etcd_renewal_success_rate_pct",
        "threshold": 99.9,
        "unit": "%",
        "comparison": "gte",   # actual >= threshold
        "description": "etcd lease heartbeat must succeed >= 99.9% of the time.",
    },
]


def evaluate_benchmarks(snapshot: BenchmarkSnapshot) -> list[BenchmarkResult]:
    """
    Evaluate all 6 benchmarks against the replay snapshot.
    Returns a list of BenchmarkResult objects for the resilience scorer.
    """
    results = []
    snapshot_dict = {
        "peak_consumer_lag_seconds": snapshot.peak_consumer_lag_seconds,
        "p99_insert_latency_ms": snapshot.p99_insert_latency_ms,
        "p99_select_latency_ms": snapshot.p99_select_latency_ms,
        "max_merkle_divergence_pct": snapshot.max_merkle_divergence_pct,
        "ponr_stability_pct": snapshot.ponr_stability_pct,
        "etcd_renewal_success_rate_pct": snapshot.etcd_renewal_success_rate_pct,
    }

    for bench in BENCHMARKS:
        actual = snapshot_dict.get(bench["metric"], 0.0)
        threshold = bench["threshold"]
        comparison = bench["comparison"]

        if comparison == "lte":
            passed = actual <= threshold
        elif comparison == "gte":
            passed = actual >= threshold
        else:
            passed = actual == threshold

        results.append(BenchmarkResult(
            name=f"{bench['id']}: {bench['name']}",
            metric=bench["metric"],
            threshold=f"{threshold} {bench['unit']}",
            actual=f"{actual:.3f} {bench['unit']}",
            passed=passed,
            weight_pts=30.0 / 6,
        ))

    return results
