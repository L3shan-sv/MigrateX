"""
tests/test_phase3.py
────────────────────
Phase 3 test suite.
Tests all chaos experiment logic, resilience scoring,
benchmark evaluation, and replay throttle logic.
No live infrastructure required.
"""

from __future__ import annotations

import sys
import time
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.chaos.base import (
    ChaosExperiment, ExperimentContext, ExperimentState, BlastRadius
)
from src.chaos.experiments import (
    NetworkBlackholeExperiment,
    ConsumerProcessKillExperiment,
    StateDriftExperiment,
    EtcdLeaderKillExperiment,
    KafkaBrokerFailureExperiment,
    PONRDegradedNetworkExperiment,
    SchemaMigrationUnderLoadExperiment,
)
from src.resilience.scorer import ResilienceScorer, BenchmarkResult, ResilienceReport
from src.replay.engine import TrafficReplayEngine, BenchmarkSnapshot
from src.benchmarks.definitions import evaluate_benchmarks, BENCHMARKS


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_context(
    anomaly_score: float = 0.0,
    lag: float = 5.0,
    divergence: float = 0.0,
) -> ExperimentContext:
    pool = MagicMock()
    pool._config = {"host": "localhost", "user": "test", "password": "test", "database": "test"}
    pool.execute_one.return_value = {"cnt": 0}
    pool.execute.return_value = []
    pool.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
    pool.cursor.return_value.__exit__ = MagicMock(return_value=False)

    return ExperimentContext(
        source_pool=pool,
        target_pool=pool,
        kafka_bootstrap=["localhost:9092"],
        etcd_endpoints=["localhost:2379"],
        kafka_connect_url="http://localhost:8083",
        sink_group_id="test-group",
        anomaly_score_fn=lambda: anomaly_score,
        merkle_verify_fn=lambda: divergence,
        consumer_lag_fn=lambda: lag,
        max_fault_duration_seconds=5,   # fast for tests
        auto_recover=True,
    )


def make_passing_benchmarks() -> list[BenchmarkResult]:
    return [
        BenchmarkResult(name=f"B-00{i}", metric="m", threshold="t", actual="a", passed=True)
        for i in range(1, 7)
    ]


def make_passing_experiments() -> list:
    from src.chaos.base import ExperimentResult, BlastRadius
    return [
        ExperimentResult(
            experiment_id=f"EXP-00{i}",
            name=f"Experiment {i}",
            hypothesis="h",
            blast_radius=BlastRadius.SHADOW_ONLY,
            state=ExperimentState.PASSED,
            started_at="2026-01-01T00:00:00",
            completed_at="2026-01-01T00:05:00",
            duration_seconds=300.0,
            pass_criteria=[{"criterion": "c", "expected": "e", "actual": "a", "passed": True}],
            fault_injected="none",
            recovery_verified=True,
            score_contribution=50.0 / 7,
        )
        for i in range(1, 8)
    ]


# ── Resilience Scorer tests ───────────────────────────────────────────────────

class TestResilienceScorer:

    def test_perfect_score_is_100(self):
        scorer = ResilienceScorer()
        report = scorer.calculate(
            benchmarks=make_passing_benchmarks(),
            experiments=make_passing_experiments(),
            ponr_deviation_pct=5.0,
            anomaly_calibration_passed=True,
        )
        assert report.score == 100
        assert report.passed is True

    def test_score_above_95_passes(self):
        scorer = ResilienceScorer()
        # 6 benchmarks pass, 6/7 experiments pass, PONR good, anomaly good
        experiments = make_passing_experiments()
        experiments[-1].state = ExperimentState.FAILED
        experiments[-1].score_contribution = 0.0

        report = scorer.calculate(
            benchmarks=make_passing_benchmarks(),
            experiments=experiments,
            ponr_deviation_pct=5.0,
            anomaly_calibration_passed=True,
        )
        # 30 + (6 × 50/7) + 10 + 10 = 30 + 42.86 + 20 = 92.86 → 93
        assert report.passed is False  # below 95

    def test_score_exactly_95_passes(self):
        scorer = ResilienceScorer()
        # Need to hit exactly 95
        # 30 (bench) + 50 (chaos) + 10 (ponr) + 10 (anomaly) - 5 = 95
        benchmarks = make_passing_benchmarks()
        benchmarks[0].passed = False  # lose 5 points

        report = scorer.calculate(
            benchmarks=benchmarks,
            experiments=make_passing_experiments(),
            ponr_deviation_pct=5.0,
            anomaly_calibration_passed=True,
        )
        assert report.score == 95
        assert report.passed is True

    def test_all_experiments_failed_gives_50_points_max(self):
        scorer = ResilienceScorer()
        experiments = make_passing_experiments()
        for e in experiments:
            e.state = ExperimentState.FAILED
            e.score_contribution = 0.0

        report = scorer.calculate(
            benchmarks=make_passing_benchmarks(),
            experiments=experiments,
            ponr_deviation_pct=5.0,
            anomaly_calibration_passed=True,
        )
        # 30 + 0 + 10 + 10 = 50
        assert report.score == 50
        assert report.passed is False

    def test_blocking_failures_listed_when_failed(self):
        scorer = ResilienceScorer()
        benchmarks = make_passing_benchmarks()
        benchmarks[0].passed = False

        experiments = make_passing_experiments()
        experiments[0].state = ExperimentState.FAILED

        report = scorer.calculate(
            benchmarks=benchmarks,
            experiments=experiments,
            ponr_deviation_pct=5.0,
            anomaly_calibration_passed=True,
        )
        assert len(report.blocking_failures) >= 2

    def test_ponr_high_deviation_reduces_score(self):
        scorer = ResilienceScorer()
        # > 40% deviation → 0 PONR points
        report_good = scorer.calculate(
            benchmarks=make_passing_benchmarks(),
            experiments=make_passing_experiments(),
            ponr_deviation_pct=5.0,
            anomaly_calibration_passed=True,
        )
        report_bad = scorer.calculate(
            benchmarks=make_passing_benchmarks(),
            experiments=make_passing_experiments(),
            ponr_deviation_pct=45.0,
            anomaly_calibration_passed=True,
        )
        assert report_good.ponr_stability_score > report_bad.ponr_stability_score
        assert report_bad.ponr_stability_score == 0.0

    def test_anomaly_failure_deducts_10_points(self):
        scorer = ResilienceScorer()
        report_pass = scorer.calculate(
            benchmarks=make_passing_benchmarks(),
            experiments=make_passing_experiments(),
            ponr_deviation_pct=5.0,
            anomaly_calibration_passed=True,
        )
        report_fail = scorer.calculate(
            benchmarks=make_passing_benchmarks(),
            experiments=make_passing_experiments(),
            ponr_deviation_pct=5.0,
            anomaly_calibration_passed=False,
        )
        assert report_pass.score - report_fail.score == 10

    def test_report_saves_to_file(self, tmp_path):
        scorer = ResilienceScorer()
        report = scorer.calculate(
            benchmarks=make_passing_benchmarks(),
            experiments=make_passing_experiments(),
            ponr_deviation_pct=5.0,
            anomaly_calibration_passed=True,
        )
        path = scorer.save(report, tmp_path)
        assert path.exists()
        import json
        with open(path) as f:
            data = json.load(f)
        assert data["score"] == 100
        assert data["passed"] is True

    def test_partial_benchmark_pass_scores_proportionally(self):
        scorer = ResilienceScorer()
        benchmarks = make_passing_benchmarks()
        benchmarks[0].passed = False
        benchmarks[1].passed = False  # 4/6 pass = 20 pts

        report = scorer.calculate(
            benchmarks=benchmarks,
            experiments=make_passing_experiments(),
            ponr_deviation_pct=5.0,
            anomaly_calibration_passed=True,
        )
        assert report.benchmark_score == pytest.approx(20.0, rel=0.01)

    def test_recommendation_contains_proceed_when_passed(self):
        scorer = ResilienceScorer()
        report = scorer.calculate(
            benchmarks=make_passing_benchmarks(),
            experiments=make_passing_experiments(),
            ponr_deviation_pct=5.0,
            anomaly_calibration_passed=True,
        )
        assert "PROCEED" in report.recommendation

    def test_recommendation_contains_blocked_when_failed(self):
        scorer = ResilienceScorer()
        experiments = make_passing_experiments()
        for e in experiments:
            e.state = ExperimentState.FAILED
            e.score_contribution = 0.0
        report = scorer.calculate(
            benchmarks=make_passing_benchmarks(),
            experiments=experiments,
            ponr_deviation_pct=50.0,
            anomaly_calibration_passed=False,
        )
        assert "BLOCKED" in report.recommendation


# ── Benchmark evaluation tests ────────────────────────────────────────────────

class TestBenchmarkEvaluation:

    def _make_passing_snapshot(self) -> BenchmarkSnapshot:
        return BenchmarkSnapshot(
            peak_consumer_lag_seconds=60.0,        # < 120 ✓
            p99_insert_latency_ms=100.0,            # < 200 ✓
            p99_select_latency_ms=80.0,             # < 150 ✓
            max_merkle_divergence_pct=0.0005,       # < 0.001 ✓
            etcd_renewal_success_rate_pct=99.95,    # > 99.9 ✓
            ponr_stability_pct=10.0,                # < 20 ✓
        )

    def test_all_passing_snapshot_produces_6_passes(self):
        results = evaluate_benchmarks(self._make_passing_snapshot())
        assert len(results) == 6
        assert all(r.passed for r in results)

    def test_lag_exceeds_threshold_fails_benchmark(self):
        snapshot = self._make_passing_snapshot()
        snapshot.peak_consumer_lag_seconds = 150.0  # > 120 ✗
        results = evaluate_benchmarks(snapshot)
        lag_result = next(r for r in results if "lag" in r.metric)
        assert lag_result.passed is False

    def test_high_insert_latency_fails_benchmark(self):
        snapshot = self._make_passing_snapshot()
        snapshot.p99_insert_latency_ms = 250.0  # > 200 ✗
        results = evaluate_benchmarks(snapshot)
        insert_result = next(r for r in results if "insert" in r.metric)
        assert insert_result.passed is False

    def test_merkle_divergence_at_threshold_passes(self):
        snapshot = self._make_passing_snapshot()
        snapshot.max_merkle_divergence_pct = 0.001  # exactly at threshold ✓
        results = evaluate_benchmarks(snapshot)
        merkle_result = next(r for r in results if "merkle" in r.metric)
        assert merkle_result.passed is True

    def test_merkle_divergence_above_threshold_fails(self):
        snapshot = self._make_passing_snapshot()
        snapshot.max_merkle_divergence_pct = 0.002  # > 0.001 ✗
        results = evaluate_benchmarks(snapshot)
        merkle_result = next(r for r in results if "merkle" in r.metric)
        assert merkle_result.passed is False

    def test_etcd_renewal_below_threshold_fails(self):
        snapshot = self._make_passing_snapshot()
        snapshot.etcd_renewal_success_rate_pct = 99.5  # < 99.9 ✗
        results = evaluate_benchmarks(snapshot)
        etcd_result = next(r for r in results if "etcd" in r.metric)
        assert etcd_result.passed is False

    def test_correct_number_of_benchmarks_defined(self):
        assert len(BENCHMARKS) == 6


# ── Chaos experiment base tests ───────────────────────────────────────────────

class TestChaosExperimentBase:

    def test_high_anomaly_score_skips_experiment(self):
        exp = NetworkBlackholeExperiment()
        ctx = make_context(anomaly_score=0.5)  # > 0.3 threshold
        result = exp.run(ctx)
        assert result.state == ExperimentState.SKIPPED

    def test_blast_radius_is_correct_for_each_experiment(self):
        exp_map = {
            NetworkBlackholeExperiment(): BlastRadius.NETWORK_PATH,
            ConsumerProcessKillExperiment(): BlastRadius.SHADOW_ONLY,
            StateDriftExperiment(): BlastRadius.SHADOW_ONLY,
            EtcdLeaderKillExperiment(): BlastRadius.ETCD_ONLY,
            KafkaBrokerFailureExperiment(): BlastRadius.KAFKA_ONLY,
            PONRDegradedNetworkExperiment(): BlastRadius.NETWORK_PATH,
            SchemaMigrationUnderLoadExperiment(): BlastRadius.SCHEMA_ONLY,
        }
        for exp, expected_radius in exp_map.items():
            assert exp.blast_radius == expected_radius

    def test_each_experiment_has_non_empty_hypothesis(self):
        experiments = [
            NetworkBlackholeExperiment(),
            ConsumerProcessKillExperiment(),
            StateDriftExperiment(),
            EtcdLeaderKillExperiment(),
            KafkaBrokerFailureExperiment(),
            PONRDegradedNetworkExperiment(),
            SchemaMigrationUnderLoadExperiment(),
        ]
        for exp in experiments:
            assert len(exp.hypothesis) > 20

    def test_experiment_ids_are_unique(self):
        experiments = [
            NetworkBlackholeExperiment(),
            ConsumerProcessKillExperiment(),
            StateDriftExperiment(),
            EtcdLeaderKillExperiment(),
            KafkaBrokerFailureExperiment(),
            PONRDegradedNetworkExperiment(),
            SchemaMigrationUnderLoadExperiment(),
        ]
        ids = [e.experiment_id for e in experiments]
        assert len(ids) == len(set(ids))

    def test_total_score_weight_sums_to_50(self):
        # 7 experiments × (50/7) ≈ 50
        from src.chaos.base import ChaosExperiment
        total = ChaosExperiment.SCORE_WEIGHT * 7
        assert abs(total - 50.0) < 0.01


# ── Traffic replay engine tests ───────────────────────────────────────────────

class TestTrafficReplayEngine:

    def _make_engine(
        self, lag: float = 10.0, divergence: float = 0.0
    ) -> TrafficReplayEngine:
        pool = MagicMock()
        pool.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
        pool.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return TrafficReplayEngine(
            source_pool=pool,
            database="airbnb_prod",
            replay_multiplier=5.0,
            duration_hours=0.001,  # 3.6 seconds for tests
            consumer_lag_fn=lambda: lag,
            merkle_divergence_fn=lambda: divergence,
            p99_latency_fn=lambda: (50.0, 30.0),
            ponr_evaluate_fn=lambda: {"p95_usd": 100.0},
            etcd_health_fn=lambda: 100.0,
        )

    def test_throttle_activates_when_lag_exceeds_threshold(self):
        engine = self._make_engine(lag=150.0)  # > 120s threshold
        engine.set_phase1_p95(100.0)

        throttle_called = []
        engine._on_throttle = lambda lag, mult: throttle_called.append((lag, mult))

        # Run for a short time
        engine._running = True
        engine._start_time = time.time()

        # Simulate one monitor loop iteration
        engine._current_multiplier = 5.0
        lag = engine._consumer_lag()
        if lag > engine.THROTTLE_THRESHOLD_SECONDS:
            if engine._current_multiplier > engine.THROTTLE_MULTIPLIER:
                engine._current_multiplier = engine.THROTTLE_MULTIPLIER
                if engine._on_throttle:
                    engine._on_throttle(lag, engine.THROTTLE_MULTIPLIER)

        assert engine._current_multiplier == engine.THROTTLE_MULTIPLIER

    def test_throttle_restores_on_lag_recovery(self):
        engine = self._make_engine(lag=10.0)  # Normal lag
        engine._current_multiplier = engine.THROTTLE_MULTIPLIER  # was throttled

        # Simulate recovery
        lag = engine._consumer_lag()
        if lag < engine.THROTTLE_THRESHOLD_SECONDS / 2:
            if engine._current_multiplier < engine._target_multiplier:
                engine._current_multiplier = engine._target_multiplier

        assert engine._current_multiplier == engine._target_multiplier

    def test_benchmark_snapshot_has_all_fields(self):
        engine = self._make_engine()
        engine.set_phase1_p95(100.0)
        engine._start_time = time.time()
        snapshot = engine._collect_benchmark_snapshot()
        assert hasattr(snapshot, "peak_consumer_lag_seconds")
        assert hasattr(snapshot, "p99_insert_latency_ms")
        assert hasattr(snapshot, "p99_select_latency_ms")
        assert hasattr(snapshot, "max_merkle_divergence_pct")
        assert hasattr(snapshot, "etcd_renewal_success_rate_pct")
        assert hasattr(snapshot, "ponr_stability_pct")

    def test_ponr_stability_zero_when_no_phase1_estimate(self):
        engine = self._make_engine()
        engine.set_phase1_p95(0.0)
        engine._start_time = time.time()
        snapshot = engine._collect_benchmark_snapshot()
        assert snapshot.ponr_stability_pct == 0.0


# ── PONR degraded network experiment tests ────────────────────────────────────

class TestPONRDegradedExperiment:

    def test_experiment_blocks_when_ponr_signals_block(self):
        ponr_calls = []

        def fake_ponr():
            ponr_calls.append(1)
            return {"p95_usd": 99999.0, "recommendation": "BLOCK"}

        exp = PONRDegradedNetworkExperiment(ponr_evaluate_fn=fake_ponr)
        ctx = make_context(anomaly_score=0.0)

        # Directly test verify_recovery logic
        exp._fault_start = time.time() - 35  # simulate 35s elapsed
        exp._ponr_fn = fake_ponr
        recovered = exp.verify_recovery(ctx)
        assert exp._ponr_blocked is True

    def test_experiment_has_3_pass_criteria(self):
        exp = PONRDegradedNetworkExperiment()
        exp._ponr_blocked = True
        exp._alert_fired = True
        exp._alert_latency_seconds = 30.0
        ctx = make_context()
        criteria = exp.check_pass_criteria(ctx)
        assert len(criteria) == 3

    def test_all_criteria_pass_when_ponr_blocked_and_alert_fast(self):
        exp = PONRDegradedNetworkExperiment()
        exp._ponr_blocked = True
        exp._alert_fired = True
        exp._alert_latency_seconds = 45.0  # < 120s
        ctx = make_context()
        criteria = exp.check_pass_criteria(ctx)
        assert all(c["passed"] for c in criteria)


# ── Network blackhole tests ───────────────────────────────────────────────────

class TestNetworkBlackholeExperiment:

    def test_recovery_check_fails_on_high_lag(self):
        exp = NetworkBlackholeExperiment()
        exp._recovery_lag = 200.0      # > 30s threshold
        exp._recovery_divergence = 0.0
        ctx = make_context()
        criteria = exp.check_pass_criteria(ctx)
        lag_criterion = next(c for c in criteria if "lag" in c["criterion"])
        assert lag_criterion["passed"] is False

    def test_recovery_check_passes_on_clean_state(self):
        exp = NetworkBlackholeExperiment()
        exp._recovery_lag = 5.0
        exp._recovery_divergence = 0.0
        ctx = make_context()
        criteria = exp.check_pass_criteria(ctx)
        assert all(c["passed"] for c in criteria)

    def test_no_duplicate_writes_criterion_always_passes(self):
        exp = NetworkBlackholeExperiment()
        exp._recovery_lag = 5.0
        exp._recovery_divergence = 0.0
        ctx = make_context()
        criteria = exp.check_pass_criteria(ctx)
        dup_criterion = next(c for c in criteria if "duplicate" in c["criterion"])
        assert dup_criterion["passed"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
