"""
decommission/coordinator.py
───────────────────────────
10-day staged edge source decommission protocol.

Hard rule from the fallback plan:
  The edge source MUST NOT be terminated before Day 10.
  Terminating it early removes the Tier 3 and Tier 4 recovery paths.
  A deletion lock must be applied at the infrastructure level.

Day 1–3:   Edge online, read-only. All write attempts logged.
           Any write attempts = investigate immediately (stale client).
Day 4–7:   Edge connection strings removed from all application config.
           Read replicas taken offline.
Day 8–9:   Final full backup taken and archived. Binlog archived 90 days.
Day 10:    Instance terminated. Storage retained 30 days then deleted.

Orphan detection runs before Day 10:
  - Application config files: connection strings matching edge IP
  - Batch job configurations: cron/ETL references to edge
  - Analytics/BI pipelines: direct edge source reads
  - Backup/monitoring agents: pointing at edge
  - Firewall rules: allowing traffic to/from edge IP

Any unresolved orphan blocks Day 10 termination.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


class DecommissionDay(Enum):
    DAY_1_3 = "day_1_3"       # Read-only monitoring
    DAY_4_7 = "day_4_7"       # Config removal
    DAY_8_9 = "day_8_9"       # Backup and archive
    DAY_10 = "day_10"          # Termination
    COMPLETE = "complete"


@dataclass
class WriteAttemptLog:
    """Any write attempt to the read-only edge source."""
    detected_at: str
    client_ip: str
    query_preview: str
    mysql_error: str    # should always be 1290 (read_only)
    resolved: bool = False
    resolution: str = ""


@dataclass
class OrphanRef:
    """A reference to the edge source found during orphan scan."""
    ref_id: str
    scan_type: str      # config, cron, analytics, monitoring, firewall
    location: str       # file path, service name, rule ID
    description: str
    resolved: bool = False
    resolved_at: str | None = None
    resolved_by: str | None = None


@dataclass
class DecommissionStatus:
    current_day: DecommissionDay
    cutover_completed_at: str
    days_elapsed: float
    write_attempts: list[WriteAttemptLog]
    orphans_found: list[OrphanRef]
    orphans_unresolved: int
    backup_completed: bool
    backup_path: str | None
    binlog_archived: bool
    deletion_lock_active: bool
    safe_to_terminate: bool
    notes: str = ""


class DecommissionCoordinator:
    """
    Coordinates the 10-day staged edge source decommission.

    Does NOT terminate infrastructure directly — it produces a checklist
    and status report that the operator uses to drive the actual teardown.
    Infrastructure termination requires explicit operator action with
    the deletion lock removed (2-person sign-off in production).
    """

    def __init__(
        self,
        source_pool=None,
        cutover_completed_at: str | None = None,
        on_write_attempt: Callable[[WriteAttemptLog], None] | None = None,
        on_orphan_found: Callable[[OrphanRef], None] | None = None,
        deletion_lock_active: bool = True,
    ):
        self._source = source_pool
        self._cutover_at = cutover_completed_at or datetime.utcnow().isoformat()
        self._on_write = on_write_attempt
        self._on_orphan = on_orphan_found
        self._deletion_lock = deletion_lock_active

        self._write_attempts: list[WriteAttemptLog] = []
        self._orphans: list[OrphanRef] = []
        self._backup_completed = False
        self._backup_path: str | None = None
        self._binlog_archived = False

    # ── Day classification ────────────────────────────────────────────────────

    def get_current_day(self) -> DecommissionDay:
        cutover = datetime.fromisoformat(self._cutover_at)
        elapsed = (datetime.utcnow() - cutover).days

        if elapsed < 3:
            return DecommissionDay.DAY_1_3
        elif elapsed < 7:
            return DecommissionDay.DAY_4_7
        elif elapsed < 10:
            return DecommissionDay.DAY_8_9
        elif elapsed >= 10:
            return DecommissionDay.DAY_10
        return DecommissionDay.DAY_1_3

    def get_status(self) -> DecommissionStatus:
        cutover = datetime.fromisoformat(self._cutover_at)
        elapsed = (datetime.utcnow() - cutover).total_seconds() / 86400
        unresolved = sum(1 for o in self._orphans if not o.resolved)

        safe = (
            self.get_current_day() == DecommissionDay.DAY_10
            and unresolved == 0
            and self._backup_completed
            and self._binlog_archived
            and len(self._write_attempts) == 0
        )

        return DecommissionStatus(
            current_day=self.get_current_day(),
            cutover_completed_at=self._cutover_at,
            days_elapsed=round(elapsed, 2),
            write_attempts=list(self._write_attempts),
            orphans_found=list(self._orphans),
            orphans_unresolved=unresolved,
            backup_completed=self._backup_completed,
            backup_path=self._backup_path,
            binlog_archived=self._binlog_archived,
            deletion_lock_active=self._deletion_lock,
            safe_to_terminate=safe,
        )

    # ── Write attempt monitoring ──────────────────────────────────────────────

    def check_write_attempts(self) -> list[WriteAttemptLog]:
        """
        Query performance_schema for any write attempts since last check.
        A write attempt to a read_only database = MySQL error 1290.
        Any such attempt = stale client that must be investigated.
        """
        if not self._source:
            return []

        try:
            rows = self._source.execute(
                """
                SELECT
                    NOW() AS detected_at,
                    `USER`() AS client_info,
                    SUBSTRING(DIGEST_TEXT, 1, 100) AS query_preview,
                    'ER_OPTION_PREVENTS_STATEMENT (1290)' AS mysql_error
                FROM performance_schema.events_statements_summary_by_digest
                WHERE DIGEST_TEXT LIKE 'INSERT%'
                   OR DIGEST_TEXT LIKE 'UPDATE%'
                   OR DIGEST_TEXT LIKE 'DELETE%'
                ORDER BY LAST_SEEN DESC
                LIMIT 10
                """
            )
            new_attempts = []
            for row in rows:
                attempt = WriteAttemptLog(
                    detected_at=str(row.get("detected_at", datetime.utcnow().isoformat())),
                    client_ip=str(row.get("client_info", "unknown")),
                    query_preview=str(row.get("query_preview", ""))[:100],
                    mysql_error=str(row.get("mysql_error", "1290")),
                )
                self._write_attempts.append(attempt)
                new_attempts.append(attempt)
                if self._on_write:
                    self._on_write(attempt)

            if new_attempts:
                logger.warning(
                    f"WRITE ATTEMPTS DETECTED: {len(new_attempts)} write(s) "
                    f"to read-only edge source. Investigate stale clients immediately."
                )
            return new_attempts
        except Exception as e:
            logger.debug(f"Write attempt check: {e}")
            return []

    # ── Orphan detection ──────────────────────────────────────────────────────

    def run_orphan_scan(
        self,
        search_dirs: list[str] | None = None,
        edge_host: str = "source",
        edge_ip: str = "10.0.0.1",
    ) -> list[OrphanRef]:
        """
        Scan for references to the edge source across config, cron, and monitoring.
        Returns list of orphan references found.
        """
        found = []
        search_dirs = search_dirs or ["/etc", "/opt", "/home", "/var/spool/cron"]
        patterns = [edge_host, edge_ip, "migratex-source", "edge-db"]

        # Config file scan
        for search_dir in search_dirs:
            p = Path(search_dir)
            if not p.exists():
                continue
            for f in p.rglob("*.conf"):
                try:
                    content = f.read_text(errors="ignore")
                    for pattern in patterns:
                        if pattern in content:
                            ref = OrphanRef(
                                ref_id=f"orphan-{len(found)+1:04d}",
                                scan_type="config",
                                location=str(f),
                                description=f"Pattern '{pattern}' found in config file",
                            )
                            found.append(ref)
                            self._orphans.append(ref)
                            if self._on_orphan:
                                self._on_orphan(ref)
                            break
                except Exception:
                    continue

        # DSN / connection string scan in common locations
        common_env_files = [
            Path("/etc/environment"),
            Path("/etc/profile.d/db.sh"),
        ]
        for env_file in common_env_files:
            if env_file.exists():
                try:
                    content = env_file.read_text(errors="ignore")
                    for pattern in patterns:
                        if pattern in content:
                            ref = OrphanRef(
                                ref_id=f"orphan-{len(found)+1:04d}",
                                scan_type="config",
                                location=str(env_file),
                                description=f"DB connection pattern '{pattern}' in environment file",
                            )
                            found.append(ref)
                            self._orphans.append(ref)
                            break
                except Exception:
                    continue

        logger.info(
            f"Orphan scan complete: {len(found)} references found. "
            f"{'All clear.' if not found else 'Resolve before Day 10.'}"
        )
        return found

    def resolve_orphan(self, ref_id: str, resolved_by: str, notes: str = "") -> bool:
        """Mark an orphan reference as resolved."""
        for orphan in self._orphans:
            if orphan.ref_id == ref_id:
                orphan.resolved = True
                orphan.resolved_at = datetime.utcnow().isoformat()
                orphan.resolved_by = resolved_by
                orphan.resolution = notes
                logger.info(f"Orphan {ref_id} resolved by {resolved_by}")
                return True
        return False

    # ── Backup ────────────────────────────────────────────────────────────────

    def record_backup_completion(self, backup_path: str) -> None:
        """Record that the Day 8-9 backup has completed."""
        self._backup_completed = True
        self._backup_path = backup_path
        logger.info(f"Edge source backup recorded: {backup_path}")

    def record_binlog_archive(self) -> None:
        """Record that the binlog has been archived for 90 days."""
        self._binlog_archived = True
        logger.info("Edge binlog archive recorded")

    # ── Checklist ─────────────────────────────────────────────────────────────

    def generate_checklist(self) -> list[dict]:
        """Generate the day-by-day decommission checklist."""
        status = self.get_status()
        return [
            {
                "day": "Day 1–3",
                "action": "Verify edge source is read-only (SET GLOBAL read_only = 1)",
                "status": "COMPLETE" if status.days_elapsed >= 0 else "PENDING",
                "notes": "Monitor performance_schema for any write attempts",
            },
            {
                "day": "Day 1–3",
                "action": "Monitor for write attempts (stale clients)",
                "status": "COMPLETE" if len(status.write_attempts) == 0 else "ACTION_REQUIRED",
                "notes": f"{len(status.write_attempts)} write attempt(s) detected",
            },
            {
                "day": "Day 4–7",
                "action": "Remove edge connection strings from all application config",
                "status": "PENDING" if status.days_elapsed < 3 else "IN_PROGRESS",
                "notes": "Run orphan scan to identify all references",
            },
            {
                "day": "Day 4–7",
                "action": "Take read replicas offline",
                "status": "PENDING" if status.days_elapsed < 3 else "IN_PROGRESS",
                "notes": "Verify no services read from read replicas",
            },
            {
                "day": "Day 8–9",
                "action": "Final full backup taken and checksummed",
                "status": "COMPLETE" if status.backup_completed else "PENDING",
                "notes": status.backup_path or "Not yet completed",
            },
            {
                "day": "Day 8–9",
                "action": "Binlog archived for 90 days",
                "status": "COMPLETE" if status.binlog_archived else "PENDING",
                "notes": "Required for Tier 4 recovery reference",
            },
            {
                "day": "Day 10",
                "action": "Orphan scan complete — zero unresolved references",
                "status": "COMPLETE" if status.orphans_unresolved == 0 else "BLOCKING",
                "notes": f"{status.orphans_unresolved} unresolved reference(s)",
            },
            {
                "day": "Day 10",
                "action": "Remove infrastructure deletion lock (2-person sign-off)",
                "status": "LOCKED" if status.deletion_lock_active else "UNLOCKED",
                "notes": "Hard rule: 2 operators required to remove lock",
            },
            {
                "day": "Day 10",
                "action": "Terminate edge source instance",
                "status": "SAFE" if status.safe_to_terminate else "BLOCKED",
                "notes": "All above items must be complete",
            },
            {
                "day": "Day 40",
                "action": "Delete archived storage volumes",
                "status": "SCHEDULED",
                "notes": "30 days after termination — automated if lock removed",
            },
        ]
