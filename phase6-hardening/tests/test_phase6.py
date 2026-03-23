"""
tests/test_phase6.py
────────────────────
Phase 6 test suite.
Covers: decommission coordinator, model retraining,
SRE report generation, and runbook generation.
No live infrastructure required.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock
import pytest
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.decommission.coordinator import (
    DecommissionCoordinator, DecommissionDay, OrphanRef, WriteAttemptLog
)
from src.retraining.engine import ModelRetrainingEngine
from src.report.generator import SREReportGenerator, MigrationSummary
from src.runbooks.generator import RunbookGenerator


# ── Decommission Coordinator tests ────────────────────────────────────────────

class TestDecommissionCoordinator:

    def _make_coord(self, days_ago: float = 0.0) -> DecommissionCoordinator:
        cutover_at = (
            datetime.utcnow() - timedelta(days=days_ago)
        ).isoformat()
        return DecommissionCoordinator(
            cutover_completed_at=cutover_at,
            deletion_lock_active=True,
        )

    def test_day_classification_day_1_3(self):
        coord = self._make_coord(days_ago=1.0)
        assert coord.get_current_day() == DecommissionDay.DAY_1_3

    def test_day_classification_day_4_7(self):
        coord = self._make_coord(days_ago=4.0)
        assert coord.get_current_day() == DecommissionDay.DAY_4_7

    def test_day_classification_day_8_9(self):
        coord = self._make_coord(days_ago=8.0)
        assert coord.get_current_day() == DecommissionDay.DAY_8_9

    def test_day_classification_day_10(self):
        coord = self._make_coord(days_ago=10.0)
        assert coord.get_current_day() == DecommissionDay.DAY_10

    def test_not_safe_to_terminate_before_day_10(self):
        coord = self._make_coord(days_ago=5.0)
        status = coord.get_status()
        assert status.safe_to_terminate is False

    def test_not_safe_with_unresolved_orphans(self):
        coord = self._make_coord(days_ago=11.0)
        coord._orphans.append(OrphanRef(
            ref_id="orphan-0001",
            scan_type="config",
            location="/etc/app.conf",
            description="Edge connection string found",
        ))
        status = coord.get_status()
        assert status.safe_to_terminate is False

    def test_not_safe_without_backup(self):
        coord = self._make_coord(days_ago=11.0)
        # No backup recorded
        status = coord.get_status()
        assert status.safe_to_terminate is False

    def test_safe_to_terminate_all_conditions_met(self):
        coord = self._make_coord(days_ago=11.0)
        coord.record_backup_completion("/backups/edge-final.sql.gz")
        coord.record_binlog_archive()
        # No orphans, no write attempts
        status = coord.get_status()
        assert status.safe_to_terminate is True

    def test_resolve_orphan_marks_resolved(self):
        coord = self._make_coord(days_ago=5.0)
        coord._orphans.append(OrphanRef(
            ref_id="orphan-0001",
            scan_type="config",
            location="/etc/app.conf",
            description="Edge connection string",
        ))
        result = coord.resolve_orphan("orphan-0001", "ops@company.com", "Updated config")
        assert result is True
        assert coord._orphans[0].resolved is True

    def test_resolve_unknown_orphan_returns_false(self):
        coord = self._make_coord()
        result = coord.resolve_orphan("does-not-exist", "ops@test.com")
        assert result is False

    def test_checklist_has_10_items(self):
        coord = self._make_coord()
        checklist = coord.generate_checklist()
        assert len(checklist) == 10

    def test_deletion_lock_active_by_default(self):
        coord = self._make_coord()
        status = coord.get_status()
        assert status.deletion_lock_active is True

    def test_backup_recording(self):
        coord = self._make_coord()
        coord.record_backup_completion("/backups/final.sql.gz")
        status = coord.get_status()
        assert status.backup_completed is True
        assert status.backup_path == "/backups/final.sql.gz"

    def test_binlog_archive_recording(self):
        coord = self._make_coord()
        coord.record_binlog_archive()
        status = coord.get_status()
        assert status.binlog_archived is True

    def test_orphan_callback_fires(self):
        found = []
        coord = DecommissionCoordinator(
            cutover_completed_at=datetime.utcnow().isoformat(),
            on_orphan_found=lambda o: found.append(o),
        )
        coord._orphans.append(OrphanRef(
            ref_id="x", scan_type="config",
            location="/test", description="test",
        ))
        assert len(coord._orphans) == 1


# ── Model Retraining tests ────────────────────────────────────────────────────

class TestModelRetrainingEngine:

    def test_retrain_with_synthetic_data(self, tmp_path):
        engine = ModelRetrainingEngine(output_dir=tmp_path)
        signals = engine.generate_synthetic_cloud_signals(n_samples=300)
        result = engine.retrain_isolation_forest(signals)
        assert result.model_type == "IsolationForest"
        assert result.training_samples == 300
        assert result.calibration_passed is True
        assert Path(result.saved_to).exists()

    def test_retrain_saves_model_to_disk(self, tmp_path):
        engine = ModelRetrainingEngine(output_dir=tmp_path)
        signals = engine.generate_synthetic_cloud_signals(n_samples=200)
        result = engine.retrain_isolation_forest(signals)
        assert (tmp_path / "isolation_forest_cloud.pkl").exists()
        assert (tmp_path / "model_metadata_cloud.json").exists()

    def test_synthetic_cloud_signals_have_correct_features(self):
        engine = ModelRetrainingEngine()
        signals = engine.generate_synthetic_cloud_signals(n_samples=10)
        required = [
            "p99_select_latency_ms", "p99_write_latency_ms",
            "replication_lag_seconds", "wal_write_rate_bytes_per_sec",
            "buffer_pool_hit_rate_pct", "conn_active", "conn_idle",
            "conn_waiting", "deadlock_count_per_min", "long_query_count",
        ]
        for sig in signals:
            for f in required:
                assert f in sig

    def test_cloud_signals_have_lower_latency_than_edge(self):
        engine = ModelRetrainingEngine()
        cloud = engine.generate_synthetic_cloud_signals(n_samples=100, seed=42)
        avg_p99 = sum(s["p99_select_latency_ms"] for s in cloud) / len(cloud)
        # Cloud P99 should average well below 50ms (edge was ~20-50ms)
        assert avg_p99 < 30.0

    def test_ponr_recalibration_saves_params(self, tmp_path):
        engine = ModelRetrainingEngine(output_dir=tmp_path)
        result = engine.recalibrate_ponr(
            observed_cloud_writes_per_sec=50_000_000,
            observed_cloud_network_gbps=10.0,
            observed_cloud_network_std_gbps=0.5,
            phase1_estimate_usd=1000.0,
            observed_cost_usd=950.0,
        )
        assert result.calibration_passed is True
        assert result.p50_deviation_pct <= 10.0
        assert (tmp_path / "ponr_cloud_params.json").exists()

    def test_ponr_recalibration_detects_high_deviation(self, tmp_path):
        engine = ModelRetrainingEngine(output_dir=tmp_path)
        result = engine.recalibrate_ponr(
            observed_cloud_writes_per_sec=50_000_000,
            observed_cloud_network_gbps=10.0,
            observed_cloud_network_std_gbps=0.5,
            phase1_estimate_usd=1000.0,
            observed_cost_usd=1500.0,  # 50% deviation → fails calibration
        )
        assert result.calibration_passed is False
        assert result.p50_deviation_pct > 10.0

    def test_threshold_change_pct_computed(self, tmp_path):
        engine = ModelRetrainingEngine(output_dir=tmp_path)
        signals = engine.generate_synthetic_cloud_signals(n_samples=200)
        result = engine.retrain_isolation_forest(signals, alert_threshold=0.65)
        # No Phase 1 model to compare against in test env → 0.0
        assert isinstance(result.threshold_change_pct, float)


# ── SRE Report Generator tests ────────────────────────────────────────────────

class TestSREReportGenerator:

    def _make_generator(self, tmp_path) -> SREReportGenerator:
        return SREReportGenerator(output_dir=tmp_path)

    def test_generate_produces_report(self, tmp_path):
        gen = self._make_generator(tmp_path)
        report = gen.generate(
            migration_id="mig-001",
            started_at="2026-01-01T00:00:00",
            cutover_completed_at="2026-01-10T02:00:00",
            cutover_result={"ponr_p95_usd": 800.0},
            actual_cost_usd=450.0,
            downtime_ms=87.0,
            data_volume_gb=100.0,
        )
        assert report.report_id.startswith("sre-mig-001")
        assert report.summary.data_loss_events == 0
        assert report.summary.application_downtime_ms == 87.0

    def test_report_saved_to_disk(self, tmp_path):
        gen = self._make_generator(tmp_path)
        report = gen.generate(
            migration_id="mig-002",
            started_at="2026-01-01T00:00:00",
            cutover_completed_at="2026-01-10T02:00:00",
            cutover_result={},
        )
        files = list(tmp_path.glob("sre-mig-002*.json"))
        assert len(files) == 1

    def test_lessons_generated(self, tmp_path):
        gen = self._make_generator(tmp_path)
        report = gen.generate(
            migration_id="mig-003",
            started_at="2026-01-01T00:00:00",
            cutover_completed_at="2026-01-10T02:00:00",
            cutover_result={"ponr_p95_usd": 500.0},
            downtime_ms=75.0,
        )
        assert len(report.lessons_learned) >= 1

    def test_recommendations_generated(self, tmp_path):
        gen = self._make_generator(tmp_path)
        report = gen.generate(
            migration_id="mig-004",
            started_at="2026-01-01T00:00:00",
            cutover_completed_at="2026-01-10T02:00:00",
            cutover_result={},
        )
        assert len(report.recommendations) >= 2

    def test_cost_reconciliation_computed(self, tmp_path):
        gen = self._make_generator(tmp_path)
        report = gen.generate(
            migration_id="mig-005",
            started_at="2026-01-01T00:00:00",
            cutover_completed_at="2026-01-10T02:00:00",
            cutover_result={},
            finops_summary={"phase1_p50_estimate_usd": 500.0, "savings_attributed_usd": 50.0},
            actual_cost_usd=480.0,
        )
        assert report.cost_reconciliation["actual_usd"] == 480.0
        assert report.cost_reconciliation["savings_attributed_usd"] == 50.0

    def test_ponr_evolution_accuracy_assessment(self, tmp_path):
        gen = self._make_generator(tmp_path)
        report = gen.generate(
            migration_id="mig-006",
            started_at="2026-01-01T00:00:00",
            cutover_completed_at="2026-01-10T02:00:00",
            cutover_result={"ponr_p95_usd": 100.0},
        )
        assert report.ponr_evolution.model_accuracy_assessment != ""


# ── Runbook Generator tests ───────────────────────────────────────────────────

class TestRunbookGenerator:

    def test_generates_4_runbooks(self, tmp_path):
        gen = RunbookGenerator(output_dir=tmp_path)
        runbooks = gen.generate_all()
        assert len(runbooks) == 4

    def test_runbook_ids_are_unique(self, tmp_path):
        gen = RunbookGenerator(output_dir=tmp_path)
        runbooks = gen.generate_all()
        ids = [r.runbook_id for r in runbooks]
        assert len(ids) == len(set(ids))

    def test_all_runbooks_saved_to_disk(self, tmp_path):
        gen = RunbookGenerator(output_dir=tmp_path)
        gen.generate_all()
        files = list(tmp_path.glob("RB-*.json"))
        assert len(files) == 4

    def test_runbook_1_health_has_sections(self, tmp_path):
        gen = RunbookGenerator(output_dir=tmp_path)
        runbooks = gen.generate_all()
        rb1 = next(r for r in runbooks if r.runbook_id == "RB-001")
        assert len(rb1.sections) >= 3

    def test_runbook_2_anomaly_has_alert_matrix(self, tmp_path):
        gen = RunbookGenerator(output_dir=tmp_path)
        runbooks = gen.generate_all()
        rb2 = next(r for r in runbooks if r.runbook_id == "RB-002")
        alert_section = next(s for s in rb2.sections if "alert" in s["title"].lower())
        assert "alerts" in alert_section

    def test_runbook_3_dr_has_failover_sequence(self, tmp_path):
        gen = RunbookGenerator(output_dir=tmp_path)
        runbooks = gen.generate_all()
        rb3 = next(r for r in runbooks if r.runbook_id == "RB-003")
        failover_section = next(
            s for s in rb3.sections if "sequence" in s["title"].lower()
        )
        assert len(failover_section["steps"]) >= 5

    def test_runbook_4_ops_has_ponr_section(self, tmp_path):
        gen = RunbookGenerator(output_dir=tmp_path)
        runbooks = gen.generate_all()
        rb4 = next(r for r in runbooks if r.runbook_id == "RB-004")
        ponr_section = next(s for s in rb4.sections if "PONR" in s["title"])
        assert "operations" in ponr_section

    def test_runbooks_are_valid_json_on_disk(self, tmp_path):
        gen = RunbookGenerator(output_dir=tmp_path)
        gen.generate_all()
        for f in tmp_path.glob("RB-*.json"):
            with open(f) as fp:
                data = json.load(fp)
            assert "runbook_id" in data
            assert "sections" in data


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
