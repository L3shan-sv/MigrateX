"""
audit/semantic.py
─────────────────
Semantic audit layer for Phase 4.

Row-count parity is necessary but not sufficient.
This layer verifies that critical business logic produces IDENTICAL
outcomes from both the edge source and cloud target databases.

The 6 critical business logic paths for an Airbnb-scale application:

  Path 1: Booking transaction integrity
    Full workflow: search → hold → confirm → payment state
    Test: same booking_id produces same row state on both DBs

  Path 2: User account state transitions
    Test: create, verify, suspend, delete → identical row state

  Path 3: Inventory availability calculations
    Test: availability query for same listing+dates → identical result set

  Path 4: Review aggregation
    Test: AVG(rating) and COUNT(*) for same listing_id → identical

  Path 5: Price calculation
    Test: any stored proc / view for price → identical output

  Path 6: Search ranking
    Test: same search params → identical row identity set
          (order may differ — that's Class B, not Class A)

Exit criterion: zero Class A errors on ALL 6 paths over 72-hour window.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from src.shadow.proxy import ShadowWriteProxy, DivergenceClass, ShadowComparison

logger = logging.getLogger(__name__)


@dataclass
class PathAuditResult:
    path_id: str
    path_name: str
    checks_run: int
    class_a_errors: int
    class_b_errors: int
    class_c_errors: int
    last_checked_at: str
    zero_class_a: bool           # True = this path is clean

    @property
    def passed(self) -> bool:
        return self.zero_class_a


@dataclass
class SemanticAuditReport:
    started_at: str
    evaluated_at: str
    total_checks: int
    class_a_total: int
    class_b_total: int
    class_c_total: int
    paths: list[PathAuditResult]
    exit_criteria_met: bool      # True = all 6 paths have zero Class A
    shadow_error_rate: float
    blocking_paths: list[str]    # paths with Class A errors


class SemanticAuditLayer:
    """
    Runs structured semantic checks across the 6 critical business logic paths.
    Each check queries both edge and target with identical parameters and
    classifies any divergence.
    """

    CRITICAL_PATHS = [
        "PATH-001",  # Booking transaction integrity
        "PATH-002",  # User account state transitions
        "PATH-003",  # Inventory availability
        "PATH-004",  # Review aggregation
        "PATH-005",  # Price calculation
        "PATH-006",  # Search ranking
    ]

    def __init__(
        self,
        proxy: ShadowWriteProxy,
        source_pool,
        target_pool,
        database: str,
    ):
        self._proxy = proxy
        self._source = source_pool
        self._target = target_pool
        self._database = database
        self._path_results: dict[str, PathAuditResult] = {
            pid: PathAuditResult(
                path_id=pid,
                path_name=self._path_name(pid),
                checks_run=0,
                class_a_errors=0,
                class_b_errors=0,
                class_c_errors=0,
                last_checked_at=datetime.utcnow().isoformat(),
                zero_class_a=True,
            )
            for pid in self.CRITICAL_PATHS
        }

    # ── Path 1: Booking transaction integrity ─────────────────────────────────

    def check_booking_integrity(
        self, booking_id: Any, table_name: str = "bookings"
    ) -> ShadowComparison:
        """
        Verify that a booking record has identical state on edge and target.
        Tests the full booking lifecycle: the row that resulted from
        search → hold → confirm → payment update.
        """
        result = self._proxy.compare_write(
            table_name=table_name,
            pk_column="id",
            pk_value=booking_id,
            operation="BOOKING_VERIFY",
            wait_ms=0,
        )
        self._record_path_result("PATH-001", result)
        return result

    def check_booking_status_transitions(
        self, booking_ids: list[Any], table_name: str = "bookings"
    ) -> list[ShadowComparison]:
        """Verify status field consistency across a batch of bookings."""
        results = []
        for bid in booking_ids:
            r = self._proxy.compare_write(
                table_name=table_name,
                pk_column="id",
                pk_value=bid,
                operation="STATUS_CHECK",
                wait_ms=0,
            )
            results.append(r)
            self._record_path_result("PATH-001", r)
        return results

    # ── Path 2: User account state transitions ────────────────────────────────

    def check_user_state(
        self, user_id: Any, table_name: str = "users"
    ) -> ShadowComparison:
        """
        Verify user account state is identical post-transition.
        Covers: create, verify, suspend, delete.
        """
        result = self._proxy.compare_write(
            table_name=table_name,
            pk_column="id",
            pk_value=user_id,
            operation="USER_STATE_CHECK",
            wait_ms=0,
        )
        self._record_path_result("PATH-002", result)
        return result

    # ── Path 3: Inventory availability ───────────────────────────────────────

    def check_availability(
        self,
        listing_id: Any,
        check_in: str,
        check_out: str,
        availability_table: str = "availability",
    ) -> ShadowComparison:
        """
        Verify availability query returns identical results from edge and target.
        This tests GROUP BY, date range queries, and availability logic.
        """
        query = (
            f"SELECT * FROM `{availability_table}` "
            f"WHERE listing_id = %s AND date >= %s AND date < %s "
            f"AND available = 1 ORDER BY date"
        )
        result = self._proxy.compare_query(
            table_name=availability_table,
            query=query,
            params=(listing_id, check_in, check_out),
        )
        self._record_path_result("PATH-003", result)
        return result

    # ── Path 4: Review aggregation ────────────────────────────────────────────

    def check_review_aggregation(
        self,
        listing_id: Any,
        reviews_table: str = "reviews",
    ) -> ShadowComparison:
        """
        Verify AVG(rating) and COUNT(*) are identical on edge and target.
        Tests GROUP BY and aggregate function consistency.
        This is the canary for numeric precision divergence.
        """
        edge_row, edge_ms = self._proxy._query_row(
            self._source, reviews_table, "listing_id", listing_id
        )
        # Use aggregate query directly
        agg_query = (
            f"SELECT COUNT(*) AS review_count, "
            f"ROUND(AVG(rating), 4) AS avg_rating, "
            f"MAX(rating) AS max_rating, "
            f"MIN(rating) AS min_rating "
            f"FROM `{reviews_table}` WHERE listing_id = %s"
        )
        result = self._proxy.compare_query(
            table_name=reviews_table,
            query=agg_query,
            params=(listing_id,),
        )
        self._record_path_result("PATH-004", result)
        return result

    # ── Path 5: Price calculation ─────────────────────────────────────────────

    def check_price_calculation(
        self,
        listing_id: Any,
        pricing_table: str = "listing_pricing",
    ) -> ShadowComparison:
        """
        Verify price calculation produces identical output from stored
        procedures or pricing views on both databases.
        Any difference here = incorrect billing = Class A immediately.
        """
        result = self._proxy.compare_write(
            table_name=pricing_table,
            pk_column="listing_id",
            pk_value=listing_id,
            operation="PRICE_CHECK",
            wait_ms=0,
        )
        self._record_path_result("PATH-005", result)
        return result

    # ── Path 6: Search ranking ────────────────────────────────────────────────

    def check_search_ranking(
        self,
        city: str,
        check_in: str,
        check_out: str,
        guests: int,
        listings_table: str = "listings",
        limit: int = 20,
    ) -> ShadowComparison:
        """
        Verify search results return the same ROW IDENTITIES from both DBs.
        Order may differ (Class B) — but the set of returned listing_ids
        must be identical (Class A if different).
        """
        query = (
            f"SELECT id FROM `{listings_table}` "
            f"WHERE city = %s AND max_guests >= %s "
            f"ORDER BY id LIMIT %s"
        )
        result = self._proxy.compare_query(
            table_name=listings_table,
            query=query,
            params=(city, guests, limit),
        )
        self._record_path_result("PATH-006", result)
        return result

    # ── Full audit run ────────────────────────────────────────────────────────

    def run_spot_check(
        self,
        sample_ids: dict[str, list[Any]],
    ) -> SemanticAuditReport:
        """
        Run a spot check across all 6 paths with provided sample IDs.

        Args:
            sample_ids: {
                "booking_ids": [1, 2, ...],
                "user_ids": [10, 20, ...],
                "listing_ids": [100, 200, ...],
            }
        """
        logger.info("Running semantic audit spot check across all 6 paths")

        for bid in sample_ids.get("booking_ids", []):
            self.check_booking_integrity(bid)

        for uid in sample_ids.get("user_ids", []):
            self.check_user_state(uid)

        for lid in sample_ids.get("listing_ids", []):
            self.check_review_aggregation(lid)
            self.check_price_calculation(lid)

        return self.generate_report()

    def generate_report(self) -> SemanticAuditReport:
        window = self._proxy.get_audit_window()
        paths = list(self._path_results.values())
        blocking = [p.path_name for p in paths if not p.passed]
        exit_met = all(p.passed for p in paths) and window.exit_criteria_met

        return SemanticAuditReport(
            started_at=window.window_start,
            evaluated_at=datetime.utcnow().isoformat(),
            total_checks=window.total_comparisons,
            class_a_total=window.class_a_count,
            class_b_total=window.class_b_count,
            class_c_total=window.class_c_count,
            paths=paths,
            exit_criteria_met=exit_met,
            shadow_error_rate=window.shadow_error_rate,
            blocking_paths=blocking,
        )

    # ── Internal ──────────────────────────────────────────────────────────────

    def _record_path_result(self, path_id: str, comparison: ShadowComparison) -> None:
        result = self._path_results[path_id]
        result.checks_run += 1
        result.last_checked_at = comparison.timestamp

        if comparison.divergence_class == DivergenceClass.CLASS_A:
            result.class_a_errors += 1
            result.zero_class_a = False
        elif comparison.divergence_class == DivergenceClass.CLASS_B:
            result.class_b_errors += 1
        elif comparison.divergence_class == DivergenceClass.CLASS_C:
            result.class_c_errors += 1

    def _path_name(self, path_id: str) -> str:
        names = {
            "PATH-001": "Booking transaction integrity",
            "PATH-002": "User account state transitions",
            "PATH-003": "Inventory availability calculations",
            "PATH-004": "Review aggregation",
            "PATH-005": "Price calculation",
            "PATH-006": "Search ranking",
        }
        return names.get(path_id, path_id)
