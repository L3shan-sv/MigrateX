"""
tests/test_phase1.py
────────────────────
Phase 1 test suite.
Tests are self-contained — no live DB required.
Uses synthetic data and mocked connections throughout.
"""

import json
import math
import sys
import pytest
import numpy as np
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ponr.engine import (
    PONREngine,
    NetworkProfile,
    MigrationState,
    RollbackCostModel,
)
from src.ml_baseline.anomaly import (
    AnomalyDetector,
    HeartbeatSignal,
    generate_synthetic_baseline,
    SIGNAL_FEATURES,
)
from src.scanner.schema import SchemaScanner, _classify_pii, ColumnInfo


# ── PONR Engine tests ─────────────────────────────────────────────────────────

class TestPONREngine:

    def _make_state(self, progress: float = 0.5) -> MigrationState:
        total = int(100e9)  # 100 GB
        return MigrationState(
            total_data_bytes=total,
            bytes_transferred=int(total * progress),
            write_rate_bytes_per_sec=50e6,  # 50 MB/s
            replication_lag_seconds=2.0,
            elapsed_seconds=3600 * progress * 24,
            last_clean_merkle_seconds=300,
        )

    def _make_network(self) -> NetworkProfile:
        return NetworkProfile(
            mean_throughput_gbps=1.0,
            std_throughput_gbps=0.1,
            rtt_ms_p50=5.0,
            rtt_ms_p99=25.0,
            packet_loss_pct=0.01,
        )

    def test_distribution_has_correct_percentile_ordering(self):
        engine = PONREngine(n_simulations=1000, sla_threshold_usd=5000.0, seed=42)
        dist = engine.evaluate(self._make_state(), self._make_network())
        assert dist.p5_usd <= dist.p50_usd <= dist.p95_usd <= dist.p99_usd

    def test_p95_below_sla_means_rollback_feasible(self):
        engine = PONREngine(n_simulations=1000, sla_threshold_usd=999_999.0, seed=42)
        dist = engine.evaluate(self._make_state(progress=0.1), self._make_network())
        assert dist.rollback_feasible is True

    def test_high_progress_increases_cost(self):
        engine = PONREngine(n_simulations=500, sla_threshold_usd=5000.0, seed=42)
        network = self._make_network()
        dist_early = engine.evaluate(self._make_state(progress=0.1), network)
        dist_late = engine.evaluate(self._make_state(progress=0.95), network)
        # Later progress = more egress cost = higher P50
        assert dist_late.p50_usd >= dist_early.p50_usd

    def test_block_recommendation_when_p95_exceeds_sla(self):
        engine = PONREngine(n_simulations=500, sla_threshold_usd=0.01, seed=42)
        dist = engine.evaluate(self._make_state(progress=0.9), self._make_network())
        assert dist.recommendation == "BLOCK"
        assert dist.rollback_feasible is False

    def test_proceed_recommendation_when_well_below_sla(self):
        engine = PONREngine(n_simulations=500, sla_threshold_usd=999_999.0, seed=42)
        dist = engine.evaluate(self._make_state(progress=0.1), self._make_network())
        assert dist.recommendation in ("PROCEED", "CAUTION")

    def test_simulations_count_in_output(self):
        n = 500
        engine = PONREngine(n_simulations=n, sla_threshold_usd=5000.0, seed=42)
        dist = engine.evaluate(self._make_state(), self._make_network())
        assert dist.simulations_run == n

    def test_event_horizon_is_positive_when_below_sla(self):
        engine = PONREngine(n_simulations=500, sla_threshold_usd=999_999.0, seed=42)
        dist = engine.evaluate(self._make_state(progress=0.1), self._make_network())
        if dist.event_horizon_seconds is not None:
            assert dist.event_horizon_seconds > 0

    def test_pre_migration_estimate_has_all_checkpoints(self):
        engine = PONREngine(n_simulations=200, sla_threshold_usd=5000.0, seed=42)
        network = self._make_network()
        estimate = engine.run_pre_migration_estimate(
            total_data_bytes=int(100e9),
            write_rate_bytes_per_sec=50e6,
            network=network,
        )
        assert len(estimate["rollback_cost_by_progress"]) == 6
        assert estimate["total_data_gb"] == pytest.approx(100.0, rel=0.01)
        assert "p50" in estimate["estimated_duration_hours"]
        assert "p95" in estimate["estimated_duration_hours"]

    def test_degraded_network_raises_p95(self):
        engine = PONREngine(n_simulations=500, sla_threshold_usd=999_999.0, seed=42)
        good_net = self._make_network()
        bad_net = NetworkProfile(
            mean_throughput_gbps=0.1,   # 10x slower
            std_throughput_gbps=0.05,
            rtt_ms_p50=100.0,
            rtt_ms_p99=500.0,
            packet_loss_pct=0.50,       # 50% packet loss
        )
        dist_good = engine.evaluate(self._make_state(), good_net)
        dist_bad = engine.evaluate(self._make_state(), bad_net)
        assert dist_bad.p95_usd >= dist_good.p95_usd

    def test_zero_bytes_transferred_gives_minimal_cost(self):
        engine = PONREngine(n_simulations=200, sla_threshold_usd=5000.0, seed=42)
        state = self._make_state(progress=0.0)
        dist = engine.evaluate(state, self._make_network())
        # Egress cost at 0% = 0. Should be very low.
        assert dist.p50_usd < 100.0


# ── Anomaly Detector tests ────────────────────────────────────────────────────

class TestAnomalyDetector:

    def test_trains_on_synthetic_data(self):
        signals = generate_synthetic_baseline(n_samples=500)
        detector = AnomalyDetector(contamination=0.05, alert_threshold=0.7)
        metadata = detector.fit(signals)
        assert metadata.training_samples == 500
        assert len(metadata.feature_names) == len(SIGNAL_FEATURES)
        assert metadata.contamination == 0.05

    def test_normal_signal_scores_below_threshold(self):
        signals = generate_synthetic_baseline(n_samples=500, seed=1)
        detector = AnomalyDetector(contamination=0.05, alert_threshold=0.7)
        detector.fit(signals)

        normal_signal = HeartbeatSignal(
            timestamp="2026-01-01T00:00:00",
            p99_select_latency_ms=25.0,
            p99_write_latency_ms=15.0,
            replication_lag_seconds=0.5,
            wal_write_rate_bytes_per_sec=40_000_000.0,
            buffer_pool_hit_rate_pct=99.0,
            conn_active=25,
            conn_idle=50,
            conn_waiting=0,
            deadlock_count_per_min=0.05,
            long_query_count=1,
        )
        result = detector.score(normal_signal)
        assert result.is_anomaly is False
        assert result.score < 0.7

    def test_anomalous_signal_scores_above_threshold(self):
        signals = generate_synthetic_baseline(n_samples=1000, seed=2)
        detector = AnomalyDetector(contamination=0.05, alert_threshold=0.6)
        detector.fit(signals)

        # Clear anomaly: extreme latency + massive replication lag
        anomalous_signal = HeartbeatSignal(
            timestamp="2026-01-01T00:00:00",
            p99_select_latency_ms=2000.0,   # catastrophic
            p99_write_latency_ms=1500.0,
            replication_lag_seconds=120.0,  # 2 minutes behind
            wal_write_rate_bytes_per_sec=500_000_000.0,  # spiking
            buffer_pool_hit_rate_pct=60.0,  # terrible
            conn_active=140,                # near max
            conn_idle=5,
            conn_waiting=80,
            deadlock_count_per_min=15.0,
            long_query_count=50,
        )
        result = detector.score(anomalous_signal)
        assert result.is_anomaly is True
        assert result.score >= 0.6

    def test_model_persists_and_loads(self, tmp_path):
        signals = generate_synthetic_baseline(n_samples=300, seed=3)
        detector = AnomalyDetector(contamination=0.05, alert_threshold=0.7)
        detector.fit(signals)
        detector.save(tmp_path)

        loaded = AnomalyDetector.load(tmp_path, alert_threshold=0.7)
        assert loaded._trained is True

        signal = generate_synthetic_baseline(n_samples=1, seed=99)[0]
        result_original = detector.score(signal)
        result_loaded = loaded.score(signal)
        assert abs(result_original.score - result_loaded.score) < 0.001

    def test_unfit_detector_raises_on_score(self):
        detector = AnomalyDetector()
        signal = generate_synthetic_baseline(n_samples=1)[0]
        with pytest.raises(RuntimeError, match="not trained"):
            detector.score(signal)

    def test_batch_scoring_matches_individual(self):
        signals = generate_synthetic_baseline(n_samples=200, seed=4)
        detector = AnomalyDetector(contamination=0.05, alert_threshold=0.7)
        detector.fit(signals)

        test_signals = generate_synthetic_baseline(n_samples=10, seed=5)
        batch_results = detector.score_batch(test_signals)
        individual_results = [detector.score(s) for s in test_signals]

        for batch, individual in zip(batch_results, individual_results):
            assert abs(batch.score - individual.score) < 0.001

    def test_normalised_scores_are_between_zero_and_one(self):
        signals = generate_synthetic_baseline(n_samples=300, seed=6)
        detector = AnomalyDetector(contamination=0.05, alert_threshold=0.7)
        detector.fit(signals)

        test_signals = generate_synthetic_baseline(n_samples=50, seed=7)
        for signal in test_signals:
            result = detector.score(signal)
            assert 0.0 <= result.score <= 1.0


# ── PII classifier tests ──────────────────────────────────────────────────────

class TestPIIClassifier:

    def _col(self, name: str, data_type: str = "varchar", max_len: int = 255) -> ColumnInfo:
        return ColumnInfo(
            name=name, data_type=data_type, column_type=f"{data_type}({max_len})",
            is_nullable=True, character_max_length=max_len, column_key="",
        )

    def test_email_column_is_tier1(self):
        is_pii, tier = _classify_pii(self._col("user_email"))
        assert is_pii is True
        assert tier == 1

    def test_phone_number_is_tier1(self):
        is_pii, tier = _classify_pii(self._col("phone_number"))
        assert is_pii is True
        assert tier == 1

    def test_zipcode_is_tier2(self):
        is_pii, tier = _classify_pii(self._col("zipcode"))
        assert is_pii is True
        assert tier == 2

    def test_long_text_is_tier3(self):
        is_pii, tier = _classify_pii(self._col("review_text", "text", 0))
        assert is_pii is True
        assert tier == 3

    def test_non_pii_column_not_classified(self):
        is_pii, tier = _classify_pii(self._col("created_at", "datetime"))
        assert is_pii is False
        assert tier is None

    def test_listing_id_not_pii(self):
        is_pii, tier = _classify_pii(self._col("listing_id", "bigint"))
        assert is_pii is False

    def test_first_name_is_tier1(self):
        is_pii, tier = _classify_pii(self._col("first_name"))
        assert is_pii is True
        assert tier == 1

    def test_latitude_is_tier2(self):
        is_pii, tier = _classify_pii(self._col("latitude", "decimal"))
        assert is_pii is True
        assert tier == 2


# ── Synthetic baseline tests ──────────────────────────────────────────────────

class TestSyntheticBaseline:

    def test_generates_correct_count(self):
        signals = generate_synthetic_baseline(n_samples=100)
        assert len(signals) == 100

    def test_all_signals_have_valid_feature_vectors(self):
        signals = generate_synthetic_baseline(n_samples=50)
        for s in signals:
            vec = s.to_feature_vector()
            assert len(vec) == len(SIGNAL_FEATURES)
            assert all(math.isfinite(v) for v in vec)

    def test_buffer_pool_hit_rate_is_valid_pct(self):
        signals = generate_synthetic_baseline(n_samples=200)
        for s in signals:
            assert 0.0 <= s.buffer_pool_hit_rate_pct <= 100.0

    def test_non_negative_counts(self):
        signals = generate_synthetic_baseline(n_samples=100)
        for s in signals:
            assert s.conn_active >= 0
            assert s.conn_idle >= 0
            assert s.conn_waiting >= 0
            assert s.long_query_count >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
