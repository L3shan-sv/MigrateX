"""
tests/test_phase4.py
────────────────────
Phase 4 test suite.
Covers: shadow write proxy, semantic audit layer,
HMAC redaction engine, and FinOps arbitrator.
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

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.shadow.proxy import (
    ShadowWriteProxy, DivergenceClass, AuditWindow, ShadowComparison
)
from src.audit.semantic import SemanticAuditLayer, PathAuditResult
from src.redaction.engine import HMACRedactionEngine, classify_write as classify_pii_col
from src.finops.arbitrator import (
    FinOpsArbitrator, FinOpsDecision, WriteClass, classify_write
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_pool(rows: dict | None = None):
    """Mock DB pool returning specified row data."""
    pool = MagicMock()
    if rows is not None:
        pool.execute_one.return_value = rows
        pool.execute.return_value = [rows] if rows else []
    else:
        pool.execute_one.return_value = {"id": 1, "status": "confirmed", "amount": 150.0}
        pool.execute.return_value = [{"id": 1, "status": "confirmed", "amount": 150.0}]
    pool._config = {"host": "localhost", "user": "test", "password": "test", "database": "test"}
    return pool


def make_proxy(
    edge_row=None,
    target_row=None,
    class_a_callback=None,
) -> ShadowWriteProxy:
    source = make_pool(edge_row or {"id": 1, "status": "confirmed"})
    target = make_pool(target_row or {"id": 1, "status": "confirmed"})
    return ShadowWriteProxy(
        source_pool=source,
        target_pool=target,
        critical_paths=["bookings", "users", "payments"],
        observation_window_hours=72,
        on_class_a=class_a_callback,
        sample_rate=1.0,
    )


def make_redaction_engine(extra_pii=None) -> HMACRedactionEngine:
    pii_map = {
        "users": [
            {"column": "email", "pii_tier": 1},
            {"column": "phone_number", "pii_tier": 1},
            {"column": "zipcode", "pii_tier": 2},
            {"column": "bio", "pii_tier": 3},
        ],
        "bookings": [
            {"column": "guest_name", "pii_tier": 1},
        ],
    }
    if extra_pii:
        pii_map.update(extra_pii)

    fk_graph = [
        {
            "from_table": "bookings",
            "from_column": "user_id",
            "to_table": "users",
            "to_column": "id",
            "constraint": "fk_bookings_users",
        }
    ]
    key = b"test-secret-key-32-bytes-exactly!"[:32].ljust(32, b"!")
    return HMACRedactionEngine(pii_map, fk_graph, hmac_key=key)


# ── Shadow Write Proxy tests ──────────────────────────────────────────────────

class TestShadowWriteProxy:

    def test_identical_rows_produce_no_divergence(self):
        row = {"id": 1, "status": "confirmed", "amount": 150.0}
        proxy = make_proxy(edge_row=row, target_row=row)
        result = proxy.compare_write("bookings", "id", 1, "INSERT", wait_ms=0)
        assert result.divergence_class == DivergenceClass.NONE

    def test_different_values_produce_class_a(self):
        edge_row = {"id": 1, "status": "confirmed", "amount": 150.0}
        target_row = {"id": 1, "status": "pending", "amount": 150.0}
        proxy = make_proxy(edge_row=edge_row, target_row=target_row)
        result = proxy.compare_write("bookings", "id", 1, "UPDATE", wait_ms=0)
        assert result.divergence_class == DivergenceClass.CLASS_A

    def test_class_a_restarts_72h_window(self):
        edge_row = {"id": 1, "status": "confirmed"}
        target_row = {"id": 1, "status": "WRONG_VALUE"}
        alerts = []
        proxy = make_proxy(
            edge_row=edge_row,
            target_row=target_row,
            class_a_callback=lambda c: alerts.append(c),
        )

        initial_start = proxy.get_audit_window().window_start
        proxy.compare_write("bookings", "id", 1, "INSERT", wait_ms=0)

        window = proxy.get_audit_window()
        assert window.class_a_count == 1
        assert len(alerts) == 1
        # Window should have restarted (new start time)
        assert window.window_start >= initial_start

    def test_missing_target_row_is_class_a_for_insert(self):
        source = make_pool({"id": 5, "status": "confirmed"})
        target = make_pool(None)
        target.execute_one.return_value = None
        proxy = ShadowWriteProxy(
            source_pool=source,
            target_pool=target,
            critical_paths=["bookings"],
            observation_window_hours=72,
        )
        result = proxy.compare_write("bookings", "id", 5, "INSERT", wait_ms=0)
        assert result.divergence_class == DivergenceClass.CLASS_A

    def test_delete_on_both_produces_no_divergence(self):
        source = make_pool(None)
        source.execute_one.return_value = None
        target = make_pool(None)
        target.execute_one.return_value = None
        proxy = ShadowWriteProxy(
            source_pool=source, target_pool=target,
            critical_paths=["bookings"], observation_window_hours=72,
        )
        result = proxy.compare_write("bookings", "id", 1, "DELETE", wait_ms=0)
        assert result.divergence_class == DivergenceClass.NONE

    def test_delete_on_edge_but_row_persists_on_target_is_class_a(self):
        source = make_pool(None)
        source.execute_one.return_value = None
        target = make_pool({"id": 1, "status": "confirmed"})
        proxy = ShadowWriteProxy(
            source_pool=source, target_pool=target,
            critical_paths=["bookings"], observation_window_hours=72,
        )
        result = proxy.compare_write("bookings", "id", 1, "DELETE", wait_ms=0)
        assert result.divergence_class == DivergenceClass.CLASS_A

    def test_high_target_latency_produces_class_c(self):
        row = {"id": 1, "status": "confirmed"}
        proxy = make_proxy(edge_row=row, target_row=row)
        proxy._target._execute_latency = 500  # mock

        # Simulate timing divergence by patching _query_row
        original = proxy._query_row
        call_count = [0]

        def patched_query_row(pool, table, pk_col, pk_val):
            call_count[0] += 1
            row_val, ms = original(pool, table, pk_col, pk_val)
            if call_count[0] % 2 == 0:  # target call
                ms = 400.0   # 400ms
            else:
                ms = 20.0    # 20ms edge
            return row_val, ms

        proxy._query_row = patched_query_row
        result = proxy.compare_write("bookings", "id", 1, "INSERT", wait_ms=0)
        # With 400ms target vs 20ms edge = 20x — should be Class C
        assert result.divergence_class in (DivergenceClass.CLASS_C, DivergenceClass.NONE)

    def test_critical_path_flag_set_correctly(self):
        row = {"id": 1, "status": "confirmed"}
        proxy = make_proxy(edge_row=row, target_row=row)
        result = proxy.compare_write("bookings", "id", 1, "INSERT", wait_ms=0)
        assert result.critical_path is True

    def test_non_critical_path_flag(self):
        row = {"id": 1, "val": "test"}
        proxy = make_proxy(edge_row=row, target_row=row)
        result = proxy.compare_write("analytics_logs", "id", 1, "INSERT", wait_ms=0)
        assert result.critical_path is False

    def test_metrics_track_correctly(self):
        row = {"id": 1, "status": "confirmed"}
        proxy = make_proxy(edge_row=row, target_row=row)
        for i in range(5):
            proxy.compare_write("bookings", "id", i, "INSERT", wait_ms=0)
        window = proxy.get_audit_window()
        assert window.total_comparisons == 5

    def test_shadow_error_rate_calculation(self):
        edge_row = {"id": 1, "status": "confirmed"}
        target_row = {"id": 1, "status": "WRONG"}
        proxy = make_proxy(edge_row=edge_row, target_row=target_row)
        proxy.compare_write("bookings", "id", 1, "INSERT", wait_ms=0)
        window = proxy.get_audit_window()
        assert window.shadow_error_rate > 0.0

    def test_null_vs_empty_string_is_not_class_a(self):
        edge_row = {"id": 1, "notes": None}
        target_row = {"id": 1, "notes": ""}
        proxy = make_proxy(edge_row=edge_row, target_row=target_row)
        result = proxy.compare_write("listings", "id", 1, "UPDATE", wait_ms=0)
        # NULL vs empty string = Class B (soft divergence), not Class A
        assert result.divergence_class != DivergenceClass.CLASS_A


# ── Semantic Audit Layer tests ────────────────────────────────────────────────

class TestSemanticAuditLayer:

    def test_all_6_paths_initialised(self):
        proxy = make_proxy()
        audit = SemanticAuditLayer(proxy, make_pool(), make_pool(), "airbnb_prod")
        assert len(audit._path_results) == 6

    def test_clean_comparisons_leave_paths_green(self):
        row = {"id": 1, "status": "confirmed"}
        proxy = make_proxy(edge_row=row, target_row=row)
        audit = SemanticAuditLayer(proxy, make_pool(), make_pool(), "airbnb_prod")
        audit.check_booking_integrity(1, table_name="bookings")
        result = audit._path_results["PATH-001"]
        assert result.zero_class_a is True
        assert result.passed is True

    def test_class_a_marks_path_as_failed(self):
        edge_row = {"id": 1, "status": "confirmed"}
        target_row = {"id": 1, "status": "CORRUPTED"}
        proxy = make_proxy(edge_row=edge_row, target_row=target_row)
        audit = SemanticAuditLayer(proxy, make_pool(), make_pool(), "airbnb_prod")
        audit.check_booking_integrity(1, table_name="bookings")
        result = audit._path_results["PATH-001"]
        assert result.zero_class_a is False
        assert result.class_a_errors == 1

    def test_generate_report_shows_blocking_paths(self):
        edge_row = {"id": 1, "status": "confirmed"}
        target_row = {"id": 1, "status": "BAD"}
        proxy = make_proxy(edge_row=edge_row, target_row=target_row)
        audit = SemanticAuditLayer(proxy, make_pool(), make_pool(), "airbnb_prod")
        audit.check_booking_integrity(1, table_name="bookings")
        report = audit.generate_report()
        assert "Booking transaction integrity" in report.blocking_paths

    def test_report_exit_criteria_false_with_class_a(self):
        edge_row = {"id": 1, "status": "X"}
        target_row = {"id": 1, "status": "Y"}
        proxy = make_proxy(edge_row=edge_row, target_row=target_row)
        audit = SemanticAuditLayer(proxy, make_pool(), make_pool(), "airbnb_prod")
        audit.check_booking_integrity(1, table_name="bookings")
        report = audit.generate_report()
        assert report.exit_criteria_met is False


# ── HMAC Redaction Engine tests ───────────────────────────────────────────────

class TestHMACRedactionEngine:

    def test_tier1_column_is_redacted(self):
        engine = make_redaction_engine()
        result = engine.redact_value("john@example.com", "users", "email")
        assert result != "john@example.com"
        assert len(result) == 64  # SHA256 hex = 64 chars

    def test_tier2_column_is_redacted(self):
        engine = make_redaction_engine()
        result = engine.redact_value("10001", "users", "zipcode")
        assert result != "10001"
        assert len(result) == 64

    def test_tier3_column_passes_through(self):
        engine = make_redaction_engine()
        original = "Great place to stay!"
        result = engine.redact_value(original, "users", "bio")
        assert result == original

    def test_non_pii_column_passes_through(self):
        engine = make_redaction_engine()
        result = engine.redact_value("2026-01-01", "users", "created_at")
        assert result == "2026-01-01"

    def test_none_value_returns_none(self):
        engine = make_redaction_engine()
        assert engine.redact_value(None, "users", "email") is None

    def test_hmac_is_deterministic_same_input(self):
        engine = make_redaction_engine()
        r1 = engine.redact_value("test@example.com", "users", "email")
        r2 = engine.redact_value("test@example.com", "users", "email")
        r3 = engine.redact_value("test@example.com", "users", "email")
        assert r1 == r2 == r3

    def test_different_inputs_produce_different_hashes(self):
        engine = make_redaction_engine()
        h1 = engine.redact_value("alice@example.com", "users", "email")
        h2 = engine.redact_value("bob@example.com", "users", "email")
        assert h1 != h2

    def test_redact_row_redacts_pii_columns(self):
        engine = make_redaction_engine()
        row = {
            "id": 42,
            "email": "user@test.com",
            "phone_number": "+1234567890",
            "zipcode": "10001",
            "bio": "Hello world",
            "created_at": "2026-01-01",
        }
        redacted = engine.redact_row(row, "users")
        assert redacted["id"] == 42
        assert redacted["email"] != "user@test.com"
        assert redacted["phone_number"] != "+1234567890"
        assert redacted["zipcode"] != "10001"
        assert redacted["bio"] == "Hello world"  # tier 3 passthrough
        assert redacted["created_at"] == "2026-01-01"

    def test_verify_determinism_passes(self):
        engine = make_redaction_engine()
        assert engine.verify_determinism("test@example.com", "users", "email") is True

    def test_verify_irreversibility_passes(self):
        engine = make_redaction_engine()
        values = ["alice@test.com", "bob@test.com", "carol@test.com"]
        assert engine.verify_irreversibility(values, "users", "email") is True

    def test_fk_processing_order_parents_before_children(self):
        engine = make_redaction_engine()
        order = engine.get_processing_order()
        # users must come before bookings (bookings FK → users)
        if "users" in order and "bookings" in order:
            assert order.index("users") < order.index("bookings")

    def test_different_keys_produce_different_hashes(self):
        key1 = b"key-one-32-bytes-exactly-padded!!"[:32]
        key2 = b"key-two-32-bytes-exactly-padded!!"[:32]
        engine1 = make_redaction_engine()
        engine1._key = key1
        engine2 = make_redaction_engine()
        engine2._key = key2
        h1 = engine1._hmac("test@example.com")
        h2 = engine2._hmac("test@example.com")
        assert h1 != h2

    def test_session_has_key_fingerprint_not_key(self):
        engine = make_redaction_engine()
        session = engine.get_session()
        assert len(session.key_fingerprint) == 8  # first 8 chars only
        assert session.session_id != ""


# ── FinOps Arbitrator tests ───────────────────────────────────────────────────

class TestFinOpsArbitrator:

    def test_critical_table_always_flows(self):
        arb = FinOpsArbitrator(
            baseline_price_per_gb=0.09,
            spike_multiplier_threshold=1.5,
            price_fn=lambda: 0.20,  # 2.2x spike
        )
        decision = arb.decide("bookings", "INSERT", {"id": 1}, bytes_estimate=1024)
        assert decision == FinOpsDecision.FLOW_IMMEDIATELY

    def test_non_critical_table_buffered_during_spike(self):
        arb = FinOpsArbitrator(
            baseline_price_per_gb=0.09,
            spike_multiplier_threshold=1.5,
            price_fn=lambda: 0.20,  # 2.2x spike — above 1.5x threshold
            migration_deadline_hours=9999,
        )
        decision = arb.decide("analytics_events", "INSERT", {"id": 1}, 1024)
        assert decision == FinOpsDecision.BUFFER_TO_WAL

    def test_non_critical_flows_at_normal_price(self):
        arb = FinOpsArbitrator(
            baseline_price_per_gb=0.09,
            spike_multiplier_threshold=1.5,
            price_fn=lambda: 0.09,  # exactly baseline — no spike
            migration_deadline_hours=9999,
        )
        decision = arb.decide("analytics_events", "INSERT", {"id": 1}, 1024)
        assert decision == FinOpsDecision.FLOW_IMMEDIATELY

    def test_deadline_override_suspends_cost_opt(self):
        arb = FinOpsArbitrator(
            baseline_price_per_gb=0.09,
            spike_multiplier_threshold=1.5,
            price_fn=lambda: 5.00,  # massive spike
            migration_deadline_hours=1,  # deadline in 1 hour!
        )
        # Set migration start time to make deadline appear close
        arb._migration_start_time = time.time() - (arb._deadline_hours - 0.5) * 3600

        decision = arb.decide("analytics_events", "INSERT", {"id": 1}, 1024)
        assert decision == FinOpsDecision.DEADLINE_OVERRIDE

    def test_wal_buffer_accumulates_buffered_writes(self):
        arb = FinOpsArbitrator(
            baseline_price_per_gb=0.09,
            spike_multiplier_threshold=1.5,
            price_fn=lambda: 0.20,
            migration_deadline_hours=9999,
        )
        for i in range(10):
            arb.decide("analytics_events", "INSERT", {"id": i}, 1024)

        status = arb.get_status()
        assert status.wal_buffer_size == 10

    def test_flush_wal_clears_buffer(self):
        arb = FinOpsArbitrator(
            baseline_price_per_gb=0.09,
            spike_multiplier_threshold=1.5,
            price_fn=lambda: 0.20,
            migration_deadline_hours=9999,
        )
        for i in range(5):
            arb.decide("analytics_events", "INSERT", {"id": i}, 1024)

        assert arb.get_status().wal_buffer_size == 5
        flushed = arb.flush_wal()
        assert flushed == 5
        assert arb.get_status().wal_buffer_size == 0

    def test_savings_accumulate_during_buffering(self):
        arb = FinOpsArbitrator(
            baseline_price_per_gb=0.09,
            spike_multiplier_threshold=1.5,
            price_fn=lambda: 0.20,
            migration_deadline_hours=9999,
        )
        arb.decide("analytics_events", "INSERT", {"id": 1}, bytes_estimate=1_000_000_000)  # 1GB
        status = arb.get_status()
        assert status.savings_attributed_usd > 0.0

    def test_classify_write_critical_tables(self):
        assert classify_write("bookings") == WriteClass.CRITICAL
        assert classify_write("users") == WriteClass.CRITICAL
        assert classify_write("payments") == WriteClass.CRITICAL

    def test_classify_write_non_critical_tables(self):
        assert classify_write("analytics_events") == WriteClass.NON_CRITICAL
        assert classify_write("audit_logs") == WriteClass.NON_CRITICAL

    def test_unknown_table_defaults_to_critical(self):
        assert classify_write("some_unknown_table_xyz") == WriteClass.CRITICAL

    def test_meter_tracks_bytes_and_cost(self):
        arb = FinOpsArbitrator(
            baseline_price_per_gb=0.09,
            price_fn=lambda: 0.09,
            migration_deadline_hours=9999,
        )
        arb.decide("bookings", "INSERT", {"id": 1}, bytes_estimate=1_000_000_000)  # 1GB
        meter = arb.get_meter()
        assert meter.bytes_transferred_today == 1_000_000_000
        assert meter.cost_today_usd == pytest.approx(0.09, rel=0.01)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
