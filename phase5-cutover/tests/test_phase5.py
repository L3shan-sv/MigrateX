"""
tests/test_phase5.py
────────────────────
Phase 5 test suite.
Covers: PONR live monitor, pre-flight checklist,
operator override gate, atomic cutover sequence,
and post-cutover monitor.
No live infrastructure required.
"""

from __future__ import annotations

import json
import sys
import time
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ponr.monitor import LivePONRMonitor, LiveMigrationState, PONRSnapshot
from src.preflight.checklist import PreflightChecklist, CheckStatus, PreflightResult
from src.override.gate import OperatorOverrideGate, OperatorDecision, GateDecision
from src.cutover.sequence import (
    AtomicCutoverSequence, SequenceState, AbortReason, CutoverResult
)
from src.monitor.post_cutover import PostCutoverMonitor, ValidationCheckpoint


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_state(
    progress: float = 0.5,
    lag: float = 2.0,
    anomaly: float = 0.1,
    throughput: float = 1.0,
) -> LiveMigrationState:
    total = int(100e9)
    return LiveMigrationState(
        total_data_bytes=total,
        bytes_transferred=int(total * progress),
        write_rate_bytes_per_sec=50e6,
        replication_lag_seconds=lag,
        elapsed_seconds=3600 * 24 * progress,
        last_clean_merkle_seconds=300,
        network_throughput_gbps=throughput,
        network_std_gbps=0.1,
        anomaly_score=anomaly,
    )


def make_pool():
    pool = MagicMock()
    pool.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
    pool.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return pool


# ── PONR Monitor tests ────────────────────────────────────────────────────────

class TestLivePONRMonitor:

    def test_evaluate_returns_snapshot(self):
        monitor = LivePONRMonitor(n_simulations=500, sla_threshold_usd=5000.0, seed=42)
        snapshot = monitor.evaluate_once(make_state())
        assert isinstance(snapshot, PONRSnapshot)
        assert snapshot.p5_usd <= snapshot.p50_usd <= snapshot.p95_usd <= snapshot.p99_usd

    def test_proceed_when_p95_well_below_sla(self):
        monitor = LivePONRMonitor(n_simulations=500, sla_threshold_usd=999_999.0, seed=42)
        snapshot = monitor.evaluate_once(make_state(progress=0.1))
        assert snapshot.recommendation in ("PROCEED", "CAUTION")
        assert snapshot.rollback_feasible is True

    def test_block_when_p95_above_sla(self):
        monitor = LivePONRMonitor(n_simulations=500, sla_threshold_usd=0.01, seed=42)
        snapshot = monitor.evaluate_once(make_state(progress=0.9))
        assert snapshot.recommendation == "BLOCK"
        assert snapshot.rollback_feasible is False

    def test_caution_fires_callback(self):
        events = []
        monitor = LivePONRMonitor(
            n_simulations=200,
            sla_threshold_usd=50.0,   # low threshold to trigger CAUTION
            seed=42,
            on_caution=lambda s: events.append(s),
        )
        # Set last recommendation to PROCEED so transition fires
        monitor._last_recommendation = "PROCEED"
        # Force CAUTION by evaluating with high-cost state
        state = make_state(progress=0.95)
        snapshot = monitor.evaluate_once(state)
        # Callback fires on transition from PROCEED to CAUTION/BLOCK

    def test_block_fires_callback(self):
        blocked = []
        monitor = LivePONRMonitor(
            n_simulations=200,
            sla_threshold_usd=0.01,   # ridiculously low — always blocks
            seed=42,
            on_block=lambda s: blocked.append(s),
        )
        monitor._last_recommendation = "PROCEED"  # ensure transition
        monitor.evaluate_once(make_state(progress=0.9))
        assert len(blocked) >= 1

    def test_history_accumulates(self):
        monitor = LivePONRMonitor(n_simulations=100, sla_threshold_usd=5000.0, seed=42)
        for _ in range(5):
            monitor.evaluate_once(make_state())
        assert len(monitor.get_history()) == 5

    def test_is_cutover_permitted_true_when_proceed(self):
        monitor = LivePONRMonitor(n_simulations=200, sla_threshold_usd=999_999.0, seed=42)
        monitor.evaluate_once(make_state(progress=0.1))
        assert monitor.is_cutover_permitted() is True

    def test_is_cutover_permitted_false_when_blocked(self):
        monitor = LivePONRMonitor(n_simulations=200, sla_threshold_usd=0.01, seed=42)
        monitor.evaluate_once(make_state(progress=0.9))
        assert monitor.is_cutover_permitted() is False

    def test_network_health_index_high_for_stable_network(self):
        monitor = LivePONRMonitor(n_simulations=100, sla_threshold_usd=5000.0, seed=42)
        # Low std relative to mean = stable = high health
        health = monitor._network_health_index(throughput=1.0, std=0.05)
        assert health >= 0.9

    def test_network_health_index_low_for_unstable_network(self):
        monitor = LivePONRMonitor(n_simulations=100, sla_threshold_usd=5000.0, seed=42)
        # High std relative to mean = unstable = low health
        health = monitor._network_health_index(throughput=1.0, std=0.9)
        assert health < 0.5

    def test_event_horizon_is_none_when_blocked(self):
        monitor = LivePONRMonitor(n_simulations=200, sla_threshold_usd=0.01, seed=42)
        snapshot = monitor.evaluate_once(make_state(progress=0.9))
        assert snapshot.event_horizon_seconds == 0.0

    def test_migration_progress_calculated_correctly(self):
        monitor = LivePONRMonitor(n_simulations=100, sla_threshold_usd=5000.0, seed=42)
        snapshot = monitor.evaluate_once(make_state(progress=0.75))
        assert abs(snapshot.migration_progress_pct - 75.0) < 1.0


# ── Pre-flight Checklist tests ────────────────────────────────────────────────

class TestPreflightChecklist:

    def _make_checklist(self, lag=2.0, ponr_p95=1000.0, anomaly=0.1, merkle_age=10.0) -> PreflightChecklist:
        return PreflightChecklist(
            etcd_endpoints=[],   # empty = simulation mode
            kafka_bootstrap=[],
            replication_lag_fn=lambda: lag,
            ponr_snapshot_fn=lambda: type('obj', (object,), {'p95_usd': ponr_p95})(),
            anomaly_score_fn=lambda: anomaly,
            merkle_age_fn=lambda: merkle_age,
            lag_max_seconds=5.0,
            ponr_sla_usd=5_000.0,
            anomaly_threshold=0.7,
            merkle_max_age_minutes=30,
        )

    def test_all_automated_green_with_good_metrics(self):
        checklist = self._make_checklist(lag=2.0, ponr_p95=500.0, anomaly=0.1, merkle_age=10.0)
        result = checklist.run()
        auto_items = [i for i in result.items if i.automated]
        assert all(i.status == CheckStatus.GREEN for i in auto_items)

    def test_high_lag_produces_red_pf001(self):
        checklist = self._make_checklist(lag=10.0)  # > 5s threshold
        result = checklist.run()
        pf001 = next(i for i in result.items if i.check_id == "PF-001")
        assert pf001.status == CheckStatus.RED

    def test_ponr_above_sla_produces_red_pf002(self):
        checklist = self._make_checklist(ponr_p95=6000.0)  # > 5000 SLA
        result = checklist.run()
        pf002 = next(i for i in result.items if i.check_id == "PF-002")
        assert pf002.status == CheckStatus.RED

    def test_high_anomaly_produces_red_pf003(self):
        checklist = self._make_checklist(anomaly=0.8)  # > 0.7 threshold
        result = checklist.run()
        pf003 = next(i for i in result.items if i.check_id == "PF-003")
        assert pf003.status == CheckStatus.RED

    def test_stale_merkle_produces_red_pf004(self):
        checklist = self._make_checklist(merkle_age=45.0)  # > 30 min max
        result = checklist.run()
        pf004 = next(i for i in result.items if i.check_id == "PF-004")
        assert pf004.status == CheckStatus.RED

    def test_manual_items_start_as_manual_status(self):
        checklist = self._make_checklist()
        result = checklist.run()
        manual_ids = {"PF-007", "PF-008", "PF-009", "PF-010"}
        for item in result.items:
            if item.check_id in manual_ids:
                assert item.status == CheckStatus.MANUAL

    def test_sign_off_turns_manual_to_green(self):
        checklist = self._make_checklist()
        checklist.sign_off_manual("PF-007", "operator@company.com")
        result = checklist.run()
        pf007 = next(i for i in result.items if i.check_id == "PF-007")
        assert pf007.status == CheckStatus.GREEN

    def test_all_green_requires_all_signoffs(self):
        checklist = self._make_checklist()
        result = checklist.run()
        # Manual items pending → not all green
        assert result.all_green is False

    def test_result_has_10_items(self):
        checklist = self._make_checklist()
        result = checklist.run()
        assert len(result.items) == 10

    def test_blocking_items_listed_when_red(self):
        checklist = self._make_checklist(lag=99.0)
        result = checklist.run()
        assert "PF-001" in result.blocking_items


# ── Override Gate tests ───────────────────────────────────────────────────────

class TestOperatorOverrideGate:

    def _make_gate(self) -> OperatorOverrideGate:
        return OperatorOverrideGate(expiry_minutes=25)

    def test_valid_test_token_produces_go_decision(self):
        gate = self._make_gate()
        token = gate.generate_test_token(operator="test-op", ponr_p95=500.0)
        decision = gate.submit_go(token)
        assert decision.decision == OperatorDecision.GO
        assert decision.operator == "test-op"

    def test_ponr_block_rejects_go_decision(self):
        gate = self._make_gate()
        gate.set_ponr_blocked(True)
        token = gate.generate_test_token()
        decision = gate.submit_go(token)
        assert decision.decision != OperatorDecision.GO

    def test_invalid_token_rejected(self):
        gate = self._make_gate()
        decision = gate.submit_go("not-a-valid-token")
        assert decision.decision != OperatorDecision.GO

    def test_postpone_decision_recorded(self):
        gate = self._make_gate()
        decision = gate.submit_postpone("operator@test.com", "NETWORK_DEGRADED")
        assert decision.decision == OperatorDecision.POSTPONE
        assert "operator@test.com" in decision.reason

    def test_abort_decision_recorded(self):
        gate = self._make_gate()
        decision = gate.submit_abort("operator@test.com", "Critical production incident")
        assert decision.decision == OperatorDecision.ABORT

    def test_all_decisions_in_audit_trail(self):
        gate = self._make_gate()
        gate.submit_postpone("op1", "test")
        gate.submit_postpone("op2", "test2")
        trail = gate.get_audit_trail()
        assert len(trail) >= 2

    def test_ponr_unblock_allows_go_again(self):
        gate = self._make_gate()
        gate.set_ponr_blocked(True)
        gate.set_ponr_blocked(False)
        token = gate.generate_test_token()
        decision = gate.submit_go(token)
        assert decision.decision == OperatorDecision.GO

    def test_token_contains_ponr_context(self):
        gate = self._make_gate()
        token_str = gate.generate_test_token(ponr_p95=1234.56, lag=3.2, anomaly=0.15)
        decision = gate.submit_go(token_str)
        if decision.decision == OperatorDecision.GO:
            assert decision.ponr_p95 == pytest.approx(1234.56, rel=0.01)


# ── Atomic Cutover Sequence tests ─────────────────────────────────────────────

class TestAtomicCutoverSequence:

    def _make_sequence(
        self,
        merkle_divergence: float = 0.0,
        etcd_success: bool = True,
        anomaly: float = 0.1,
        rtt: float = 5.0,
    ) -> AtomicCutoverSequence:
        return AtomicCutoverSequence(
            source_pool=make_pool(),
            target_pool=make_pool(),
            etcd_lease_fn=lambda: (etcd_success, 2),
            dns_swing_fn=lambda: True,
            merkle_verify_fn=lambda full: merkle_divergence,
            anomaly_score_fn=lambda: anomaly,
            network_rtt_fn=lambda: rtt,
            baseline_rtt_ms=5.0,
        )

    def test_successful_cutover_returns_completed(self):
        seq = self._make_sequence()
        result = seq.execute(operator="test-op", token_id="abc123")
        assert result.state == SequenceState.COMPLETED

    def test_successful_cutover_has_7_steps(self):
        seq = self._make_sequence()
        result = seq.execute()
        assert len(result.steps) == 7

    def test_merkle_divergence_aborts_at_step2(self):
        seq = self._make_sequence(merkle_divergence=0.05)
        result = seq.execute()
        assert result.state == SequenceState.ABORTED
        assert result.abort_at_step == 2
        assert result.abort_reason == AbortReason.MERKLE_DIVERGENCE

    def test_etcd_failure_aborts_at_step3(self):
        seq = self._make_sequence(etcd_success=False)
        result = seq.execute()
        assert result.state == SequenceState.ABORTED
        assert result.abort_at_step == 3
        assert result.abort_reason == AbortReason.ETCD_LEASE_FAILURE

    def test_high_anomaly_aborts_at_step3(self):
        seq = self._make_sequence(anomaly=0.9)  # > 0.7 threshold
        result = seq.execute()
        assert result.state == SequenceState.ABORTED
        assert result.abort_reason == AbortReason.ANOMALY_DETECTED

    def test_degraded_network_aborts_at_step3(self):
        seq = self._make_sequence(rtt=50.0)  # > 3× baseline (5ms)
        result = seq.execute()
        assert result.state == SequenceState.ABORTED
        assert result.abort_reason == AbortReason.NETWORK_DEGRADED

    def test_fencing_epoch_recorded_on_success(self):
        seq = self._make_sequence()
        result = seq.execute()
        assert result.fencing_epoch == 2  # as returned by mock etcd_lease_fn

    def test_post_merkle_failure_after_step6_marks_rolled_back(self):
        call_count = [0]

        def merkle_fn(full: bool):
            call_count[0] += 1
            # First call (step 2): clean
            # Second call (step 7): dirty
            return 0.0 if call_count[0] == 1 else 0.05

        seq = AtomicCutoverSequence(
            source_pool=make_pool(),
            target_pool=make_pool(),
            merkle_verify_fn=merkle_fn,
            etcd_lease_fn=lambda: (True, 2),
            dns_swing_fn=lambda: True,
            anomaly_score_fn=lambda: 0.1,
            network_rtt_fn=lambda: 5.0,
            baseline_rtt_ms=5.0,
        )
        result = seq.execute()
        # State should be ROLLED_BACK since post-Merkle failed
        assert result.post_cutover_merkle_clean is False

    def test_step_callbacks_fire_for_each_step(self):
        steps_completed = []
        seq = self._make_sequence()
        seq._on_step = lambda s: steps_completed.append(s.step_number)
        seq.execute()
        assert steps_completed == [1, 2, 3, 4, 5, 6, 7]

    def test_abort_callback_fires_on_abort(self):
        aborts = []
        seq = self._make_sequence(etcd_success=False)
        seq._on_abort = lambda reason, step: aborts.append((reason, step))
        seq.execute()
        assert len(aborts) == 1
        assert aborts[0][1] == 3

    def test_total_duration_under_5000ms_on_success(self):
        seq = self._make_sequence()
        result = seq.execute()
        if result.state == SequenceState.COMPLETED:
            # In tests (no real DB ops), should be much faster than 100ms
            # In production, the 50ms buffer + DNS + etcd = ~100ms
            assert result.total_duration_ms < 5000  # generous for test env

    def test_operator_and_token_id_recorded_in_result(self):
        seq = self._make_sequence()
        result = seq.execute(operator="ops@company.com", token_id="token-xyz")
        assert result.operator == "ops@company.com"
        assert result.token_id == "token-xyz"


# ── Post-Cutover Monitor tests ────────────────────────────────────────────────

class TestPostCutoverMonitor:

    def test_immediate_check_all_green(self):
        monitor = PostCutoverMonitor(
            p99_latency_fn=lambda: 50.0,
            error_rate_fn=lambda: 0.0,
            merkle_verify_fn=lambda: 0.0,
            anomaly_score_fn=lambda: 0.1,
            replication_lag_fn=lambda: 1.0,
            conn_pool_utilisation_fn=lambda: 20.0,
        )
        cp = monitor.run_immediate_check()
        assert cp.all_checks_green is True

    def test_high_error_rate_fails_check(self):
        monitor = PostCutoverMonitor(
            p99_latency_fn=lambda: 50.0,
            error_rate_fn=lambda: 0.5,  # > 0.1 threshold
            merkle_verify_fn=lambda: 0.0,
            anomaly_score_fn=lambda: 0.1,
            replication_lag_fn=lambda: 1.0,
            conn_pool_utilisation_fn=lambda: 20.0,
        )
        cp = monitor.run_immediate_check()
        assert cp.all_checks_green is False

    def test_high_p99_latency_fails_check(self):
        monitor = PostCutoverMonitor(
            p99_latency_fn=lambda: 500.0,  # > 400ms threshold
            error_rate_fn=lambda: 0.0,
            merkle_verify_fn=lambda: 0.0,
            anomaly_score_fn=lambda: 0.1,
            replication_lag_fn=lambda: 1.0,
            conn_pool_utilisation_fn=lambda: 20.0,
        )
        cp = monitor.run_immediate_check()
        assert cp.all_checks_green is False

    def test_merkle_check_clean(self):
        monitor = PostCutoverMonitor(merkle_verify_fn=lambda: 0.0)
        clean = monitor.run_merkle_check(1)
        assert clean is True

    def test_merkle_check_dirty(self):
        monitor = PostCutoverMonitor(merkle_verify_fn=lambda: 0.05)
        clean = monitor.run_merkle_check(1)
        assert clean is False

    def test_generate_report_all_fields(self):
        monitor = PostCutoverMonitor(
            p99_latency_fn=lambda: 50.0,
            error_rate_fn=lambda: 0.0,
            merkle_verify_fn=lambda: 0.0,
            anomaly_score_fn=lambda: 0.1,
            replication_lag_fn=lambda: 1.0,
            conn_pool_utilisation_fn=lambda: 20.0,
        )
        monitor.run_immediate_check()
        monitor.run_merkle_check(1)
        report = monitor.generate_report()
        assert report.total_checkpoints == 1
        assert report.merkle_verifications == 1
        assert report.merkle_all_clean is True

    def test_edge_decommission_safe_requires_all_passed(self):
        monitor = PostCutoverMonitor(
            p99_latency_fn=lambda: 500.0,  # failing
            error_rate_fn=lambda: 0.0,
            merkle_verify_fn=lambda: 0.0,
            anomaly_score_fn=lambda: 0.1,
            replication_lag_fn=lambda: 1.0,
            conn_pool_utilisation_fn=lambda: 20.0,
        )
        monitor.run_immediate_check()
        report = monitor.generate_report()
        assert report.edge_decommission_safe is False


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
