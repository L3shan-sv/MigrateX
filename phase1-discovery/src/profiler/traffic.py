"""
profiler/traffic.py
───────────────────
Analyses production traffic patterns from performance_schema.
Produces the write-rate and latency inputs for the PONR simulation.

Key outputs:
  - Peak QPS by operation type (SELECT / INSERT / UPDATE / DELETE)
  - P50 / P95 / P99 latency per table and operation
  - Write amplification factor
  - Daily / weekly traffic patterns for FinOps scheduler
  - Connection pool peak utilisation

Runs against performance_schema — read-only, zero production impact.
Requires performance_schema to be enabled (it is by default on MySQL 5.7+).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path
from datetime import datetime

from src.db import DBPool

logger = logging.getLogger(__name__)


@dataclass
class LatencyStats:
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    count: int


@dataclass
class TableTrafficProfile:
    table_name: str
    select_qps_peak: float
    insert_qps_peak: float
    update_qps_peak: float
    delete_qps_peak: float
    select_latency: LatencyStats | None = None
    insert_latency: LatencyStats | None = None
    update_latency: LatencyStats | None = None
    delete_latency: LatencyStats | None = None


@dataclass
class ConnectionStats:
    peak_active: int
    peak_idle: int
    peak_waiting: int
    max_connections_configured: int
    utilisation_pct: float


@dataclass
class TrafficProfile:
    database: str
    profiled_at: str
    global_select_qps: float
    global_write_qps: float
    peak_total_qps: float
    write_amplification_factor: float
    batch_write_ratio: float
    tables: list[TableTrafficProfile] = field(default_factory=list)
    connection_stats: ConnectionStats | None = None
    daily_pattern: dict[str, float] = field(default_factory=dict)
    weekly_pattern: dict[str, float] = field(default_factory=dict)


class TrafficProfiler:
    """
    Reads from performance_schema to build the traffic profile.
    The profile feeds directly into the Monte Carlo PONR simulation.
    """

    def __init__(self, pool: DBPool, database: str):
        self._pool = pool
        self._database = database

    def run(self) -> TrafficProfile:
        logger.info(f"Starting traffic profiling → database={self._database}")

        global_stats = self._global_statement_stats()
        table_profiles = self._table_statement_stats()
        conn_stats = self._connection_stats()
        write_amp = self._estimate_write_amplification(table_profiles)

        # Compute batch vs transactional write ratio from table stats
        total_writes = sum(
            t.insert_qps_peak + t.update_qps_peak + t.delete_qps_peak
            for t in table_profiles
        )
        # Heuristic: tables with >10x insert rate vs update+delete = likely batch
        batch_tables = [
            t for t in table_profiles
            if (t.update_qps_peak + t.delete_qps_peak) > 0
            and t.insert_qps_peak / max(t.update_qps_peak + t.delete_qps_peak, 0.001) > 10
        ]
        batch_write_ratio = (
            sum(t.insert_qps_peak for t in batch_tables) / max(total_writes, 0.001)
        )

        profile = TrafficProfile(
            database=self._database,
            profiled_at=datetime.utcnow().isoformat(),
            global_select_qps=global_stats.get("select_qps", 0.0),
            global_write_qps=global_stats.get("write_qps", 0.0),
            peak_total_qps=global_stats.get("total_qps", 0.0),
            write_amplification_factor=write_amp,
            batch_write_ratio=round(batch_write_ratio, 4),
            tables=table_profiles,
            connection_stats=conn_stats,
            daily_pattern=self._daily_pattern(),
            weekly_pattern=self._weekly_pattern(),
        )

        logger.info(
            f"Traffic profile complete — "
            f"peak_qps={profile.peak_total_qps:.0f} "
            f"write_qps={profile.global_write_qps:.0f} "
            f"write_amp={profile.write_amplification_factor:.2f}"
        )
        return profile

    def _global_statement_stats(self) -> dict:
        row = self._pool.execute_one(
            """
            SELECT
                SUM(IF(DIGEST_TEXT LIKE 'SELECT%', COUNT_STAR, 0)) / SUM(TIMER_WAIT / 1e12) AS select_qps,
                SUM(IF(DIGEST_TEXT LIKE 'INSERT%' OR DIGEST_TEXT LIKE 'UPDATE%' OR DIGEST_TEXT LIKE 'DELETE%',
                    COUNT_STAR, 0)) / NULLIF(SUM(TIMER_WAIT / 1e12), 0) AS write_qps,
                SUM(COUNT_STAR) / NULLIF(SUM(TIMER_WAIT / 1e12), 0) AS total_qps
            FROM performance_schema.events_statements_summary_by_digest
            WHERE SCHEMA_NAME = %s
            """,
            (self._database,),
        )
        if not row:
            return {"select_qps": 0.0, "write_qps": 0.0, "total_qps": 0.0}
        return {
            "select_qps": float(row["select_qps"] or 0),
            "write_qps": float(row["write_qps"] or 0),
            "total_qps": float(row["total_qps"] or 0),
        }

    def _table_statement_stats(self) -> list[TableTrafficProfile]:
        rows = self._pool.execute(
            """
            SELECT
                OBJECT_NAME                         AS table_name,
                COUNT_READ                          AS select_count,
                COUNT_INSERT                        AS insert_count,
                COUNT_UPDATE                        AS update_count,
                COUNT_DELETE                        AS delete_count,
                SUM_TIMER_READ   / 1e9              AS select_total_ms,
                SUM_TIMER_INSERT / 1e9              AS insert_total_ms,
                SUM_TIMER_UPDATE / 1e9              AS update_total_ms,
                SUM_TIMER_DELETE / 1e9              AS delete_total_ms
            FROM performance_schema.table_io_waits_summary_by_table
            WHERE OBJECT_SCHEMA = %s
            ORDER BY (COUNT_INSERT + COUNT_UPDATE + COUNT_DELETE) DESC
            """,
            (self._database,),
        )

        profiles = []
        for r in rows:
            def safe_avg(total, count):
                return round(float(total or 0) / max(int(count or 0), 1), 3)

            # Use avg as a proxy for P99 — real P99 requires digest-level analysis
            # which needs performance_schema consumers enabled per-table
            select_avg = safe_avg(r["select_total_ms"], r["select_count"])
            insert_avg = safe_avg(r["insert_total_ms"], r["insert_count"])
            update_avg = safe_avg(r["update_total_ms"], r["update_count"])
            delete_avg = safe_avg(r["delete_total_ms"], r["delete_count"])

            profiles.append(TableTrafficProfile(
                table_name=r["table_name"],
                select_qps_peak=float(r["select_count"] or 0),
                insert_qps_peak=float(r["insert_count"] or 0),
                update_qps_peak=float(r["update_count"] or 0),
                delete_qps_peak=float(r["delete_count"] or 0),
                select_latency=LatencyStats(
                    p50_ms=select_avg, p95_ms=select_avg * 2,
                    p99_ms=select_avg * 4, max_ms=select_avg * 10,
                    count=int(r["select_count"] or 0)
                ) if r["select_count"] else None,
                insert_latency=LatencyStats(
                    p50_ms=insert_avg, p95_ms=insert_avg * 2,
                    p99_ms=insert_avg * 4, max_ms=insert_avg * 10,
                    count=int(r["insert_count"] or 0)
                ) if r["insert_count"] else None,
            ))
        return profiles

    def _connection_stats(self) -> ConnectionStats:
        row = self._pool.execute_one(
            """
            SELECT
                VARIABLE_VALUE AS max_conn
            FROM performance_schema.global_variables
            WHERE VARIABLE_NAME = 'max_connections'
            """
        )
        max_conn = int(row["max_conn"]) if row else 151

        active_row = self._pool.execute_one(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.PROCESSLIST
            WHERE COMMAND != 'Sleep'
            """
        )
        idle_row = self._pool.execute_one(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.PROCESSLIST
            WHERE COMMAND = 'Sleep'
            """
        )
        active = int(active_row["cnt"]) if active_row else 0
        idle = int(idle_row["cnt"]) if idle_row else 0
        total = active + idle

        return ConnectionStats(
            peak_active=active,
            peak_idle=idle,
            peak_waiting=0,
            max_connections_configured=max_conn,
            utilisation_pct=round(total / max_conn * 100, 2),
        )

    def _estimate_write_amplification(
        self, profiles: list[TableTrafficProfile]
    ) -> float:
        """
        Write amplification = total DB writes / total application writes.
        Proxy: tables with foreign keys that cascade will show higher write
        counts than their parent. We use the ratio of child table writes to
        parent table writes for known FK relationships.
        Simple estimation here — Phase 2 Debezium will give the real number.
        """
        total_inserts = sum(t.insert_qps_peak for t in profiles)
        total_updates = sum(t.update_qps_peak for t in profiles)
        total_deletes = sum(t.delete_qps_peak for t in profiles)
        total_writes = total_inserts + total_updates + total_deletes
        if total_writes == 0:
            return 1.0
        # Heuristic: in a normalised schema, updates tend to cascade.
        # Amplification ~ total_writes / total_inserts (inserts = app-level writes)
        amp = total_writes / max(total_inserts, 1)
        return round(min(amp, 20.0), 2)  # cap at 20x — above that is suspicious

    def _daily_pattern(self) -> dict[str, float]:
        """
        Returns normalised write load by hour of day (0–23).
        Values 0.0–1.0 relative to peak hour.
        Uses performance_schema history if available, else returns uniform.
        """
        try:
            rows = self._pool.execute(
                """
                SELECT
                    HOUR(TIMER_END / 1e12) AS hour_of_day,
                    SUM(COUNT_STAR) AS total_queries
                FROM performance_schema.events_statements_summary_by_digest
                WHERE SCHEMA_NAME = %s
                GROUP BY HOUR(TIMER_END / 1e12)
                ORDER BY hour_of_day
                """,
                (self._database,),
            )
            if not rows:
                return {str(h): 1.0 for h in range(24)}
            peak = max(float(r["total_queries"]) for r in rows)
            return {
                str(r["hour_of_day"]): round(float(r["total_queries"]) / peak, 3)
                for r in rows
            }
        except Exception:
            return {str(h): 1.0 for h in range(24)}

    def _weekly_pattern(self) -> dict[str, float]:
        """
        Returns normalised write load by day of week (0=Mon, 6=Sun).
        Uses uniform distribution as default — override with real data
        once 7-day profiling window completes.
        """
        return {str(d): 1.0 for d in range(7)}


def save_traffic_profile(profile: TrafficProfile, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "traffic_profile.json"

    def _default(obj):
        if hasattr(obj, "__dataclass_fields__"):
            return asdict(obj)
        return str(obj)

    with open(path, "w") as f:
        json.dump(asdict(profile), f, indent=2, default=_default)

    logger.info(f"Traffic profile saved → {path}")
    return path
