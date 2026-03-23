"""
runbooks/generator.py
─────────────────────
Generates the 4 operational runbooks produced in Phase 6.

These runbooks are the platform team's inheritance after the migration.
They encode everything learned during the migration into actionable
procedures for ongoing operations.

Runbook 1: Cloud DB health
  Normal operations: dashboard interpretation, alert response,
  capacity scaling, backup and restore.

Runbook 2: Anomaly response
  Per-alert playbooks. Escalation paths. Communication templates.
  Rollback decision criteria.

Runbook 3: DR failover
  Cloud-to-cloud regional failover using recalibrated PONR.
  Parameterized with cloud-internal network characteristics.

Runbook 4: MigrateX operational reference
  How to operate MigrateX post-migration: PONR engine, Merkle
  verification, cost reports, model threshold updates.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class Runbook:
    runbook_id: str
    title: str
    version: str
    generated_at: str
    owner: str
    sections: list[dict]


class RunbookGenerator:
    """
    Generates all 4 operational runbooks as structured JSON documents.
    In production, these would be rendered to Confluence/Notion/PDF.
    """

    def __init__(self, output_dir: str | Path = "./artifacts/runbooks"):
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def generate_all(
        self,
        alert_thresholds: dict | None = None,
        pagerduty_team: str = "platform-sre",
        cloud_db_host: str = "cloud-db.internal",
        anomaly_threshold: float = 0.7,
        ponr_sla_usd: float = 5_000.0,
    ) -> list[Runbook]:
        """Generate all 4 runbooks and save to disk."""
        runbooks = [
            self._runbook_cloud_health(cloud_db_host, pagerduty_team),
            self._runbook_anomaly_response(pagerduty_team, anomaly_threshold, alert_thresholds),
            self._runbook_dr_failover(ponr_sla_usd),
            self._runbook_migratex_ops(anomaly_threshold, ponr_sla_usd),
        ]
        for rb in runbooks:
            self._save(rb)
        logger.info(f"Generated {len(runbooks)} runbooks → {self._output_dir}")
        return runbooks

    # ── Runbook 1: Cloud DB Health ────────────────────────────────────────────

    def _runbook_cloud_health(self, db_host: str, team: str) -> Runbook:
        return Runbook(
            runbook_id="RB-001",
            title="Cloud Database Health Runbook",
            version="1.0",
            generated_at=datetime.utcnow().isoformat(),
            owner=team,
            sections=[
                {
                    "title": "1. Monitoring dashboard",
                    "content": (
                        "Primary dashboard: Grafana → MigrateX Cloud Operations. "
                        "Key panels: replication lag (should be 0 post-migration), "
                        "anomaly score (normal: < 0.3), connection pool utilisation, "
                        "buffer pool hit rate (target: > 99%), P99 latency."
                    ),
                    "alert_panels": [
                        "migratex_anomaly_score > 0.7 → HIGH",
                        "migratex_replication_lag_seconds > 60 → MEDIUM",
                        "mysql_buffer_pool_hit_rate < 95 → MEDIUM",
                    ],
                },
                {
                    "title": "2. Capacity scaling",
                    "content": (
                        "Scale up when: buffer pool hit rate < 95% sustained 30min, "
                        "connection pool utilisation > 70% sustained 15min, "
                        "P99 INSERT > 300ms sustained 10min. "
                        "Scale down: never within 30 days of cutover. "
                        "Always scale up one instance class at a time."
                    ),
                    "procedure": [
                        "1. Check current instance class in cloud console",
                        "2. Verify scale-up will not cause > 2 minute restart",
                        "3. Run PONR simulation to confirm rollback cost stays below SLA",
                        "4. Apply scale-up during lowest traffic window",
                        "5. Monitor P99 latency for 30 minutes post-scale",
                    ],
                },
                {
                    "title": "3. Backup and restore",
                    "content": (
                        "Automated daily backups at 03:00 UTC. "
                        "Retention: 35 days. "
                        "Point-in-time recovery: enabled, 5-minute granularity. "
                        "Restore test: mandatory every 90 days to staging environment."
                    ),
                    "restore_procedure": [
                        "1. Identify target restore time from incident timeline",
                        "2. Confirm restore is to staging (NEVER to production without IC approval)",
                        "3. Initiate PITR restore from cloud console",
                        "4. Run Merkle verification against latest production snapshot",
                        "5. Validate business logic on staging before promoting",
                    ],
                },
                {
                    "title": "4. Connection pool management",
                    "content": (
                        "Cloud DB max_connections: configured in Phase 2 capacity model. "
                        "Application pool idle timeout: 30 seconds (set during Phase 5). "
                        "If pool exhaustion: check for long-running queries first. "
                        "kill <pid> only with IC approval during business hours."
                    ),
                },
            ],
        )

    # ── Runbook 2: Anomaly Response ───────────────────────────────────────────

    def _runbook_anomaly_response(
        self,
        team: str,
        anomaly_threshold: float,
        thresholds: dict | None,
    ) -> Runbook:
        return Runbook(
            runbook_id="RB-002",
            title="Anomaly Response Runbook",
            version="1.0",
            generated_at=datetime.utcnow().isoformat(),
            owner=team,
            sections=[
                {
                    "title": "1. Alert severity matrix",
                    "alerts": [
                        {
                            "alert": "migratex_anomaly_score",
                            "threshold": f"> {anomaly_threshold}",
                            "severity": "HIGH",
                            "page": "PagerDuty — primary on-call",
                            "ack_window": "15 minutes",
                            "first_action": "Check Grafana anomaly dashboard. Identify which signals spiked.",
                        },
                        {
                            "alert": "migratex_replication_lag_seconds",
                            "threshold": "> 120s",
                            "severity": "HIGH",
                            "page": "PagerDuty — primary on-call",
                            "ack_window": "15 minutes",
                            "first_action": "Run SHOW PROCESSLIST on cloud DB. Check for lock contention.",
                        },
                        {
                            "alert": "migratex_fencing_violations_total",
                            "threshold": "> 0",
                            "severity": "CRITICAL",
                            "page": "PagerDuty — immediate + IC",
                            "ack_window": "5 minutes",
                            "first_action": "Stop all writes immediately. Escalate to IC. Potential split-brain.",
                        },
                        {
                            "alert": "migratex_merkle_divergence_pct",
                            "threshold": "> 0.01%",
                            "severity": "CRITICAL",
                            "page": "PagerDuty — immediate + IC",
                            "ack_window": "5 minutes",
                            "first_action": "Block all phase advancement. Trigger Merkle repair engine.",
                        },
                    ],
                },
                {
                    "title": "2. Anomaly score playbook",
                    "steps": [
                        "1. Open Grafana anomaly dashboard — identify which of the 10 features spiked",
                        "2. Cross-reference with SHOW PROCESSLIST — look for slow queries",
                        "3. If P99 latency spiked: check buffer pool hit rate. If < 95% → scale up.",
                        "4. If replication lag spiked: check Kafka consumer lag. If > 120s → scale workers.",
                        "5. If connection pool pressure: identify top connection holders → kill idle > 10min",
                        "6. If score drops below threshold within 15min → log and close",
                        "7. If score persists > 30min → page IC. Consider PONR evaluation.",
                    ],
                },
                {
                    "title": "3. Rollback decision criteria",
                    "content": (
                        "Rollback is ONLY considered if: "
                        "(1) PONR P95 is still below SLA threshold (Tier 3 possible), "
                        "(2) the incident cannot be resolved in < 60 minutes, "
                        "(3) the IC has approved the rollback decision. "
                        "Do not initiate rollback without IC approval. "
                        "Reference the fallback plan (FALLBACK_PLAN.md) for tier-specific procedures."
                    ),
                },
                {
                    "title": "4. Communication templates",
                    "templates": {
                        "incident_start": "[MIGRATEX INCIDENT] {severity} — {alert_name} triggered at {time}. "
                                         "IC: {ic_name}. Bridge: {bridge_url}. Status: investigating.",
                        "incident_update": "[MIGRATEX UPDATE] {severity} — {description}. "
                                           "ETA to resolution: {eta}. Customers affected: {impact}.",
                        "incident_resolved": "[MIGRATEX RESOLVED] {severity} — {alert_name} resolved at {time}. "
                                             "Duration: {duration}. Root cause: {rc}. Post-mortem: {pm_link}.",
                    },
                },
            ],
        )

    # ── Runbook 3: DR Failover ────────────────────────────────────────────────

    def _runbook_dr_failover(self, ponr_sla_usd: float) -> Runbook:
        return Runbook(
            runbook_id="RB-003",
            title="DR Failover Runbook",
            version="1.0",
            generated_at=datetime.utcnow().isoformat(),
            owner="platform-sre",
            sections=[
                {
                    "title": "1. Trigger conditions",
                    "content": (
                        "This runbook is for cloud-to-cloud regional failover. "
                        "Triggers: primary region unavailable > 15 minutes, "
                        "or data corruption in primary region. "
                        "NOT for edge→cloud rollback (see fallback plan Tier 3/4)."
                    ),
                },
                {
                    "title": "2. Pre-failover PONR evaluation",
                    "content": (
                        "Run PONR simulation with cloud-internal network parameters "
                        "from ponr_cloud_params.json (Phase 6 recalibration artifact). "
                        f"Block failover if P95 > ${ponr_sla_usd:,.0f} SLA threshold."
                    ),
                    "command": "python -m migratex.ponr evaluate --config ponr_cloud_params.json",
                },
                {
                    "title": "3. Failover sequence",
                    "steps": [
                        "1. Confirm primary region is unreachable (ping + console)",
                        "2. Run PONR evaluation against DR region",
                        "3. IC approves failover with signed JWT token",
                        "4. Promote DR replica to primary (cloud console or CLI)",
                        "5. Update DNS records to DR region endpoint (TTL=1s pre-set)",
                        "6. Verify application connectivity within 60 seconds",
                        "7. Run Merkle verification against last known-good snapshot",
                        "8. Monitor P99 latency for 30 minutes",
                        "9. Declare failover complete. Schedule primary region recovery.",
                    ],
                },
                {
                    "title": "4. Recovery back to primary",
                    "content": (
                        "Only recover to primary after root cause is identified and resolved. "
                        "Run the full Phase 5 pre-flight checklist against the primary. "
                        "Recovery uses the same atomic cutover sequence as the original migration."
                    ),
                },
            ],
        )

    # ── Runbook 4: MigrateX Operational Reference ─────────────────────────────

    def _runbook_migratex_ops(self, anomaly_threshold: float, ponr_sla: float) -> Runbook:
        return Runbook(
            runbook_id="RB-004",
            title="MigrateX Operational Reference",
            version="1.0",
            generated_at=datetime.utcnow().isoformat(),
            owner="platform-sre",
            sections=[
                {
                    "title": "1. PONR engine",
                    "operations": {
                        "evaluate": "python -m migratex.ponr evaluate --config ponr_cloud_params.json",
                        "update_sla": f"Edit ponr_cloud_params.json → sla_threshold_usd (current: ${ponr_sla:,.0f})",
                        "view_history": "python -m migratex.ponr history --last 50",
                    },
                },
                {
                    "title": "2. Anomaly detector",
                    "operations": {
                        "score_live": "python -m migratex.anomaly score --from-prometheus",
                        "update_threshold": f"Edit model_metadata_cloud.json → alert_threshold (current: {anomaly_threshold}). Requires model recalibration to take effect.",
                        "retrain": "python -m migratex.anomaly retrain --days 14 --source cloud",
                        "calibrate": "python -m migratex.anomaly calibrate --inject-known-faults",
                    },
                },
                {
                    "title": "3. Merkle integrity engine",
                    "operations": {
                        "full_verify": "python -m migratex.merkle verify --full",
                        "partial_verify": "python -m migratex.merkle verify --tables bookings,users",
                        "repair": "python -m migratex.merkle repair --from-wal --since <gtid>",
                        "schedule": "Merkle full verify runs automatically every 6 hours via cron",
                    },
                },
                {
                    "title": "4. Cost reporting",
                    "operations": {
                        "daily_report": "python -m migratex.finops report --period daily",
                        "monthly_report": "python -m migratex.finops report --period monthly",
                        "projection": "python -m migratex.finops project --months 3",
                    },
                },
                {
                    "title": "5. GTID deduplication table maintenance",
                    "operations": {
                        "purge": "DELETE FROM _migratex_gtid_dedup WHERE applied_at < DATE_SUB(NOW(), INTERVAL 30 DAY);",
                        "check_size": "SELECT COUNT(*) FROM _migratex_gtid_dedup;",
                        "schedule": "Purge runs automatically via scheduled maintenance every Sunday 02:00 UTC",
                    },
                },
                {
                    "title": "6. Emergency procedures",
                    "procedures": {
                        "stop_all_writes": "SET GLOBAL read_only = 1; (requires IC approval)",
                        "restart_consumer": "docker restart migratex-sink",
                        "check_fencing": "etcdctl get /migratex/fencing_epoch",
                        "check_lease": "etcdctl get /migratex/write_authority",
                        "force_merkle": "python -m migratex.merkle verify --full --urgent",
                    },
                },
            ],
        )

    def _save(self, runbook: Runbook) -> Path:
        path = self._output_dir / f"{runbook.runbook_id}_{runbook.title.replace(' ', '_')}.json"
        with open(path, "w") as f:
            json.dump({
                "runbook_id": runbook.runbook_id,
                "title": runbook.title,
                "version": runbook.version,
                "generated_at": runbook.generated_at,
                "owner": runbook.owner,
                "sections": runbook.sections,
            }, f, indent=2)
        return path
