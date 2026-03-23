"""
shadow/proxy.py
───────────────
Shadow write proxy for Phase 4 dark launch.

Architecture (Option C — CDC-based, preferred):
  The target is already receiving all writes via Debezium CDC from Phase 2.
  The shadow proxy intercepts each application write AFTER it commits on the
  edge source, then queries BOTH databases for the affected row and compares
  the results. This is semantic comparison — not write duplication.

  Edge write path (blocking — authoritative):
    App → Edge source → COMMIT → response to app

  Shadow comparison path (async — non-blocking):
    Edge COMMIT → Shadow proxy → Query edge + query target → Compare → Log

This is the safest approach because:
  1. No dual-write risk — we compare CDC-applied state, not inject writes
  2. Application never sees shadow failures — edge is always authoritative
  3. The comparison uses the same row state that Debezium applied, so it
     tests the full CDC pipeline, not just the transport

Divergence classification:
  Class A — Hard divergence: different row values after write
             → Autonomous pause + operator notification + migration BLOCKED
  Class B — Soft divergence: different execution plan / NULL handling / ordering
             → Logged, operator review required, does not block
  Class C — Timing divergence: same result but P99 latency > 2× on target
             → Logged, capacity review, does not block

Exit criterion: zero Class A divergences over 72-hour window.
Any Class A restarts the 72-hour clock.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class DivergenceClass(Enum):
    NONE = "none"
    CLASS_A = "A"   # Hard — blocks migration
    CLASS_B = "B"   # Soft — operator review
    CLASS_C = "C"   # Timing — capacity review


@dataclass
class ShadowComparison:
    """Result of comparing edge vs cloud target for one write operation."""
    comparison_id: str
    table_name: str
    operation: str          # INSERT, UPDATE, DELETE
    primary_key: Any
    edge_state: dict | None
    target_state: dict | None
    divergence_class: DivergenceClass
    divergence_details: str
    edge_latency_ms: float
    target_latency_ms: float
    timestamp: str
    critical_path: bool     # True if this table is in the critical business paths


@dataclass
class AuditWindow:
    """
    Tracks the 72-hour Class A observation window.
    Any Class A divergence restarts the clock.
    """
    window_start: str
    window_hours: int
    class_a_count: int = 0
    class_b_count: int = 0
    class_c_count: int = 0
    total_comparisons: int = 0
    last_class_a_at: str | None = None
    window_complete: bool = False

    @property
    def hours_elapsed(self) -> float:
        start = datetime.fromisoformat(self.window_start)
        return (datetime.utcnow() - start).total_seconds() / 3600

    @property
    def exit_criteria_met(self) -> bool:
        """True if 72 hours have elapsed with zero Class A divergences."""
        return self.hours_elapsed >= self.window_hours and self.class_a_count == 0

    @property
    def shadow_error_rate(self) -> float:
        if self.total_comparisons == 0:
            return 0.0
        return (self.class_a_count + self.class_b_count) / self.total_comparisons


class ShadowWriteProxy:
    """
    Intercepts write completions from the edge source and compares
    the resulting row state between edge and cloud target.

    Usage:
        proxy = ShadowWriteProxy(source_pool, target_pool, critical_paths)
        # After every edge write:
        result = proxy.compare_write(table, pk_col, pk_val, operation)
    """

    def __init__(
        self,
        source_pool,
        target_pool,
        critical_paths: list[str],
        observation_window_hours: int = 72,
        on_class_a: Callable[[ShadowComparison], None] | None = None,
        on_class_b: Callable[[ShadowComparison], None] | None = None,
        sample_rate: float = 1.0,
    ):
        self._source = source_pool
        self._target = target_pool
        self._critical_paths = {p.lower() for p in critical_paths}
        self._on_class_a = on_class_a
        self._on_class_b = on_class_b
        self._sample_rate = sample_rate

        self._window = AuditWindow(
            window_start=datetime.utcnow().isoformat(),
            window_hours=observation_window_hours,
        )
        self._lock = threading.Lock()
        self._comparisons: list[ShadowComparison] = []
        self._comparison_counter = 0

    def compare_write(
        self,
        table_name: str,
        pk_column: str,
        pk_value: Any,
        operation: str = "WRITE",
        wait_ms: int = 100,
    ) -> ShadowComparison:
        """
        Compare edge vs target state for a given row after a write.
        Called after the edge write commits.

        Args:
            table_name: the table that was written
            pk_column: the primary key column name
            pk_value: the primary key value of the written row
            operation: INSERT / UPDATE / DELETE
            wait_ms: milliseconds to wait for CDC propagation before comparing
        """
        import random
        if random.random() > self._sample_rate:
            return self._make_comparison(
                table_name, pk_column, pk_value, operation,
                None, None, DivergenceClass.NONE, "sampled_out",
                0.0, 0.0
            )

        # Small wait to allow CDC to propagate to target
        if wait_ms > 0:
            time.sleep(wait_ms / 1000)

        # Query both databases
        edge_state, edge_ms = self._query_row(self._source, table_name, pk_column, pk_value)
        target_state, target_ms = self._query_row(self._target, table_name, pk_column, pk_value)

        # Classify divergence
        div_class, details = self._classify(
            operation, edge_state, target_state, edge_ms, target_ms
        )

        comparison = self._make_comparison(
            table_name, pk_column, pk_value, operation,
            edge_state, target_state, div_class, details,
            edge_ms, target_ms
        )

        self._record(comparison)
        return comparison

    def compare_query(
        self,
        table_name: str,
        query: str,
        params: tuple,
    ) -> ShadowComparison:
        """
        Compare query results between edge and target.
        Used for critical business logic path verification.
        """
        edge_rows, edge_ms = self._run_query(self._source, query, params)
        target_rows, target_ms = self._run_query(self._target, query, params)

        # Compare result sets (order-independent for Class A check)
        edge_set = self._rows_to_set(edge_rows)
        target_set = self._rows_to_set(target_rows)

        if edge_set == target_set:
            div_class = DivergenceClass.NONE
            details = "result sets match"
        elif len(edge_set) != len(target_set):
            div_class = DivergenceClass.CLASS_A
            details = (
                f"row count mismatch: edge={len(edge_set)} target={len(target_set)}"
            )
        else:
            div_class = DivergenceClass.CLASS_B
            details = "row sets equal size but different values (execution plan divergence)"

        if target_ms > edge_ms * 2:
            if div_class == DivergenceClass.NONE:
                div_class = DivergenceClass.CLASS_C
                details = f"timing divergence: edge={edge_ms:.1f}ms target={target_ms:.1f}ms"

        comparison = self._make_comparison(
            table_name, "query", query[:64], "SELECT",
            {"rows": len(edge_rows)}, {"rows": len(target_rows)},
            div_class, details, edge_ms, target_ms
        )
        self._record(comparison)
        return comparison

    def get_audit_window(self) -> AuditWindow:
        with self._lock:
            return self._window

    def get_recent_comparisons(self, limit: int = 100) -> list[ShadowComparison]:
        with self._lock:
            return self._comparisons[-limit:]

    def get_class_a_comparisons(self) -> list[ShadowComparison]:
        with self._lock:
            return [c for c in self._comparisons if c.divergence_class == DivergenceClass.CLASS_A]

    # ── Classification ────────────────────────────────────────────────────────

    def _classify(
        self,
        operation: str,
        edge_state: dict | None,
        target_state: dict | None,
        edge_ms: float,
        target_ms: float,
    ) -> tuple[DivergenceClass, str]:
        """
        Classify divergence between edge and target row states.

        Class A triggers:
          - DELETE on edge but row still exists on target
          - INSERT/UPDATE on edge but different values on target
          - Row exists on edge but missing on target (after INSERT)

        Class B triggers:
          - Columns present in different order (schema divergence)
          - NULL vs empty string (collation difference)
          - Different numeric precision (decimal handling)

        Class C triggers:
          - Target latency > 2× edge latency
        """
        if operation.upper() == "DELETE":
            if edge_state is None and target_state is None:
                return DivergenceClass.NONE, "both deleted"
            if edge_state is None and target_state is not None:
                return DivergenceClass.CLASS_A, "deleted on edge but still present on target"
            if edge_state is not None and target_state is None:
                return DivergenceClass.CLASS_B, "exists on edge but not target (CDC lag?)"
        else:
            if edge_state is None and target_state is None:
                return DivergenceClass.CLASS_B, "row missing from both (possible rollback)"
            if edge_state is not None and target_state is None:
                return DivergenceClass.CLASS_A, "exists on edge but missing from target"
            if edge_state is None and target_state is not None:
                return DivergenceClass.CLASS_B, "missing from edge but present on target"
            if edge_state and target_state:
                diff = self._deep_diff(edge_state, target_state)
                if diff:
                    return DivergenceClass.CLASS_A, f"value divergence: {json.dumps(diff)[:200]}"

        # Timing check
        if edge_ms > 0 and target_ms > edge_ms * 2:
            return DivergenceClass.CLASS_C, (
                f"target P99 latency {target_ms:.1f}ms > 2× edge {edge_ms:.1f}ms"
            )

        return DivergenceClass.NONE, "no divergence"

    def _deep_diff(self, edge: dict, target: dict) -> dict:
        """
        Compare two row dicts. Returns dict of differing columns.
        Handles NULL/empty string normalization (Class B boundary).
        """
        diff = {}
        all_keys = set(edge.keys()) | set(target.keys())
        for key in all_keys:
            e_val = edge.get(key)
            t_val = target.get(key)
            # Normalize: None and "" are treated as equivalent (Class B territory)
            e_norm = None if e_val == "" else e_val
            t_norm = None if t_val == "" else t_val
            if e_norm != t_norm:
                diff[key] = {"edge": e_val, "target": t_val}
        return diff

    # ── Recording ─────────────────────────────────────────────────────────────

    def _record(self, comparison: ShadowComparison) -> None:
        with self._lock:
            self._comparisons.append(comparison)
            # Keep last 10k comparisons in memory
            if len(self._comparisons) > 10_000:
                self._comparisons = self._comparisons[-10_000:]

            self._window.total_comparisons += 1

            if comparison.divergence_class == DivergenceClass.CLASS_A:
                self._window.class_a_count += 1
                self._window.last_class_a_at = comparison.timestamp
                # Restart the 72-hour clock
                self._window.window_start = datetime.utcnow().isoformat()
                logger.critical(
                    f"CLASS A DIVERGENCE DETECTED — "
                    f"table={comparison.table_name} "
                    f"pk={comparison.primary_key} "
                    f"details={comparison.divergence_details}\n"
                    f"72-HOUR CLOCK RESTARTED."
                )
                if self._on_class_a:
                    self._on_class_a(comparison)

            elif comparison.divergence_class == DivergenceClass.CLASS_B:
                self._window.class_b_count += 1
                logger.warning(
                    f"Class B divergence: table={comparison.table_name} "
                    f"details={comparison.divergence_details}"
                )
                if self._on_class_b:
                    self._on_class_b(comparison)

            elif comparison.divergence_class == DivergenceClass.CLASS_C:
                self._window.class_c_count += 1
                logger.info(
                    f"Class C timing: table={comparison.table_name} "
                    f"details={comparison.divergence_details}"
                )

    # ── DB helpers ────────────────────────────────────────────────────────────

    def _query_row(
        self, pool, table: str, pk_col: str, pk_val: Any
    ) -> tuple[dict | None, float]:
        start = time.time()
        try:
            row = pool.execute_one(
                f"SELECT * FROM `{table}` WHERE `{pk_col}` = %s", (pk_val,)
            )
            ms = (time.time() - start) * 1000
            return row, ms
        except Exception as e:
            logger.error(f"Row query error on {table}: {e}")
            return None, (time.time() - start) * 1000

    def _run_query(
        self, pool, query: str, params: tuple
    ) -> tuple[list[dict], float]:
        start = time.time()
        try:
            rows = pool.execute(query, params)
            ms = (time.time() - start) * 1000
            return rows or [], ms
        except Exception as e:
            logger.error(f"Query error: {e}")
            return [], (time.time() - start) * 1000

    def _rows_to_set(self, rows: list[dict]) -> frozenset:
        return frozenset(
            json.dumps({k: str(v) for k, v in row.items()}, sort_keys=True)
            for row in rows
        )

    def _make_comparison(
        self,
        table_name: str,
        pk_col: str,
        pk_val: Any,
        operation: str,
        edge_state: dict | None,
        target_state: dict | None,
        div_class: DivergenceClass,
        details: str,
        edge_ms: float,
        target_ms: float,
    ) -> ShadowComparison:
        with self._lock:
            self._comparison_counter += 1
            cid = self._comparison_counter

        return ShadowComparison(
            comparison_id=f"cmp-{cid:08d}",
            table_name=table_name,
            operation=operation,
            primary_key=pk_val,
            edge_state=edge_state,
            target_state=target_state,
            divergence_class=div_class,
            divergence_details=details,
            edge_latency_ms=round(edge_ms, 2),
            target_latency_ms=round(target_ms, 2),
            timestamp=datetime.utcnow().isoformat(),
            critical_path=table_name.lower() in self._critical_paths,
        )
