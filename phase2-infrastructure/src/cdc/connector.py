"""
cdc/connector.py
────────────────
Manages the Debezium MySQL source connector via Kafka Connect REST API.

Responsibilities:
  - Register the Debezium connector with correct binlog configuration
  - Verify connector health and replication position
  - Monitor connector lag and publish to Prometheus
  - Handle connector restarts on failure

Debezium requires on source MySQL:
  binlog_format = ROW
  binlog_row_image = FULL
  gtid_mode = ON
  enforce_gtid_consistency = ON
  expire_logs_days >= 7

The connector streams every committed write from the source binlog
into per-table Kafka topics: migratex.{database}.{table}
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger(__name__)


@dataclass
class ConnectorStatus:
    name: str
    state: str           # RUNNING, PAUSED, FAILED, UNASSIGNED
    tasks: list[dict]
    lag_seconds: float
    gtid_position: str | None
    is_healthy: bool


class DebeziumConnectorManager:
    """
    Manages the Debezium MySQL connector lifecycle via Kafka Connect REST API.
    All operations are idempotent — safe to call multiple times.
    """

    CONNECTOR_CONFIG_TEMPLATE = {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "database.hostname": None,          # filled at runtime
        "database.port": None,
        "database.user": None,
        "database.password": None,
        "database.server.id": "5400",       # must differ from source server-id
        "topic.prefix": None,
        "database.include.list": None,
        "schema.history.internal.kafka.bootstrap.servers": None,
        "schema.history.internal.kafka.topic": "migratex.schema-history",
        "include.schema.changes": "true",
        "snapshot.mode": "initial",
        "snapshot.locking.mode": "minimal",  # minimal lock during snapshot
        "binlog.buffer.size": "0",
        "database.ssl.mode": "preferred",
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "tombstones.on.delete": "false",
        "heartbeat.interval.ms": "10000",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true",
    }

    def __init__(
        self,
        connect_url: str,
        source_host: str,
        source_port: int,
        source_user: str,
        source_password: str,
        source_database: str,
        topic_prefix: str,
        kafka_bootstrap: str,
        connector_name: str = "migratex-source",
        timeout_seconds: int = 30,
    ):
        self._url = connect_url.rstrip("/")
        self._name = connector_name
        self._timeout = timeout_seconds
        self._config = self._build_config(
            source_host, source_port, source_user, source_password,
            source_database, topic_prefix, kafka_bootstrap,
        )

    def _build_config(
        self,
        host: str, port: int, user: str, password: str,
        database: str, prefix: str, kafka_bootstrap: str,
    ) -> dict:
        cfg = dict(self.CONNECTOR_CONFIG_TEMPLATE)
        cfg["database.hostname"] = host
        cfg["database.port"] = str(port)
        cfg["database.user"] = user
        cfg["database.password"] = password
        cfg["database.include.list"] = database
        cfg["topic.prefix"] = prefix
        cfg["schema.history.internal.kafka.bootstrap.servers"] = kafka_bootstrap
        return cfg

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def register(self) -> bool:
        """
        Register (or update) the connector.
        Idempotent — updates config if connector already exists.
        """
        existing = self._get_connector()
        if existing:
            logger.info(f"Connector '{self._name}' exists — updating config")
            return self._update_connector()
        else:
            logger.info(f"Registering connector '{self._name}'")
            return self._create_connector()

    def _create_connector(self) -> bool:
        payload = {"name": self._name, "config": self._config}
        try:
            r = requests.post(
                f"{self._url}/connectors",
                json=payload,
                timeout=self._timeout,
            )
            r.raise_for_status()
            logger.info(f"Connector '{self._name}' created successfully")
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to create connector: {e}")
            return False

    def _update_connector(self) -> bool:
        try:
            r = requests.put(
                f"{self._url}/connectors/{self._name}/config",
                json=self._config,
                timeout=self._timeout,
            )
            r.raise_for_status()
            logger.info(f"Connector '{self._name}' config updated")
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to update connector: {e}")
            return False

    def pause(self) -> bool:
        try:
            r = requests.put(
                f"{self._url}/connectors/{self._name}/pause",
                timeout=self._timeout,
            )
            r.raise_for_status()
            logger.info(f"Connector '{self._name}' paused")
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to pause connector: {e}")
            return False

    def resume(self) -> bool:
        try:
            r = requests.put(
                f"{self._url}/connectors/{self._name}/resume",
                timeout=self._timeout,
            )
            r.raise_for_status()
            logger.info(f"Connector '{self._name}' resumed")
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to resume connector: {e}")
            return False

    def restart(self) -> bool:
        try:
            r = requests.post(
                f"{self._url}/connectors/{self._name}/restart?includeTasks=true",
                timeout=self._timeout,
            )
            r.raise_for_status()
            logger.info(f"Connector '{self._name}' restarted")
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to restart connector: {e}")
            return False

    def delete(self) -> bool:
        try:
            r = requests.delete(
                f"{self._url}/connectors/{self._name}",
                timeout=self._timeout,
            )
            if r.status_code == 404:
                return True  # already gone
            r.raise_for_status()
            logger.info(f"Connector '{self._name}' deleted")
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to delete connector: {e}")
            return False

    # ── Status ────────────────────────────────────────────────────────────────

    def get_status(self) -> ConnectorStatus | None:
        try:
            r = requests.get(
                f"{self._url}/connectors/{self._name}/status",
                timeout=self._timeout,
            )
            if r.status_code == 404:
                return None
            r.raise_for_status()
            data = r.json()

            connector_state = data.get("connector", {}).get("state", "UNKNOWN")
            tasks = data.get("tasks", [])
            task_states = [t.get("state") for t in tasks]
            all_running = all(s == "RUNNING" for s in task_states) if task_states else False
            is_healthy = connector_state == "RUNNING" and all_running

            return ConnectorStatus(
                name=self._name,
                state=connector_state,
                tasks=tasks,
                lag_seconds=self._get_lag_seconds(),
                gtid_position=self._get_gtid_position(),
                is_healthy=is_healthy,
            )
        except requests.RequestException as e:
            logger.error(f"Failed to get connector status: {e}")
            return None

    def wait_for_running(self, timeout_seconds: int = 120) -> bool:
        """Block until connector is RUNNING or timeout."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            status = self.get_status()
            if status and status.is_healthy:
                logger.info(f"Connector '{self._name}' is RUNNING")
                return True
            if status and status.state == "FAILED":
                logger.error(f"Connector '{self._name}' FAILED")
                return False
            logger.debug(f"Waiting for connector... state={status.state if status else 'unknown'}")
            time.sleep(5)
        logger.error(f"Connector '{self._name}' did not reach RUNNING within {timeout_seconds}s")
        return False

    def verify_binlog_config(self, pool) -> dict[str, bool]:
        """
        Verify required binlog settings on the source MySQL.
        Returns a dict of setting → pass/fail.
        All must be True before the connector is registered.
        """
        checks = {}
        required = {
            "binlog_format": "ROW",
            "binlog_row_image": "FULL",
            "gtid_mode": "ON",
            "enforce_gtid_consistency": "ON",
        }

        for var, expected in required.items():
            row = pool.execute_one(
                "SHOW VARIABLES LIKE %s", (var,)
            )
            if row:
                actual = row.get("Value", "").upper()
                expected_upper = expected.upper()
                passed = actual == expected_upper
                checks[var] = passed
                if not passed:
                    logger.error(
                        f"Binlog config check FAILED: {var}={actual} "
                        f"(expected {expected_upper})"
                    )
                else:
                    logger.info(f"Binlog config OK: {var}={actual}")
            else:
                checks[var] = False
                logger.error(f"Binlog variable not found: {var}")

        # Check expire_logs_days >= 7
        row = pool.execute_one("SHOW VARIABLES LIKE 'expire_logs_days'")
        if row:
            days = int(row.get("Value", "0") or 0)
            checks["expire_logs_days"] = days >= 7
            if days < 7:
                logger.error(f"expire_logs_days={days} (must be >= 7)")
        else:
            checks["expire_logs_days"] = False

        return checks

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get_connector(self) -> dict | None:
        try:
            r = requests.get(
                f"{self._url}/connectors/{self._name}",
                timeout=self._timeout,
            )
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            return None

    def _get_lag_seconds(self) -> float:
        """
        Estimate replication lag from connector offsets.
        Uses the Kafka Connect offsets topic — best-effort.
        """
        try:
            r = requests.get(
                f"{self._url}/connectors/{self._name}/offsets",
                timeout=self._timeout,
            )
            if r.status_code == 200:
                data = r.json()
                ts = data.get("offsets", [{}])[0].get("offset", {}).get("ts_ms", 0)
                if ts:
                    lag = max(0.0, (time.time() * 1000 - ts) / 1000)
                    return round(lag, 2)
        except Exception:
            pass
        return 0.0

    def _get_gtid_position(self) -> str | None:
        try:
            r = requests.get(
                f"{self._url}/connectors/{self._name}/offsets",
                timeout=self._timeout,
            )
            if r.status_code == 200:
                data = r.json()
                offsets = data.get("offsets", [{}])
                if offsets:
                    return offsets[0].get("offset", {}).get("gtid_set")
        except Exception:
            pass
        return None
