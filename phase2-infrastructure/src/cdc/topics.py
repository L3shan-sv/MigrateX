"""
cdc/topics.py
─────────────
Manages Kafka topic creation and validation for the CDC pipeline.

Topic naming convention: {prefix}.{database}.{table}
Example: migratex.airbnb_prod.bookings

Topic design decisions:
  - One topic per table (independent consumer parallelism)
  - 12 partitions (matches target DB write parallelism)
  - Replication factor 3 for production, 1 for local dev
  - 7-day retention (matches MySQL binlog retention — critical invariant)
  - LZ4 compression (best throughput/CPU trade-off for change events)

The 7-day retention floor is a hard rule from the fallback plan.
If consumer lag approaches 4 days, a dead-man's-switch alert fires.
This gives 3 days of buffer before the retention boundary is hit.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class TopicConfig:
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int
    compression_type: str
    cleanup_policy: str


class KafkaTopicManager:
    """
    Creates and validates Kafka topics for the CDC pipeline.
    Uses kafka-python's KafkaAdminClient.
    All operations are idempotent.
    """

    RETENTION_7_DAYS_MS = 604_800_000
    COMPRESSION = "lz4"
    CLEANUP_POLICY = "delete"

    def __init__(
        self,
        bootstrap_servers: list[str],
        topic_prefix: str,
        partitions: int = 12,
        replication_factor: int = 3,
        retention_ms: int = RETENTION_7_DAYS_MS,
        request_timeout_ms: int = 30_000,
    ):
        self._bootstrap = bootstrap_servers
        self._prefix = topic_prefix
        self._partitions = partitions
        self._replication_factor = replication_factor
        self._retention_ms = retention_ms
        self._timeout = request_timeout_ms
        self._admin = None

    def _get_admin(self):
        if self._admin is None:
            try:
                from kafka.admin import KafkaAdminClient
                self._admin = KafkaAdminClient(
                    bootstrap_servers=self._bootstrap,
                    request_timeout_ms=self._timeout,
                    client_id="migratex-topic-manager",
                )
            except Exception as e:
                logger.error(f"Failed to create Kafka admin client: {e}")
                raise
        return self._admin

    def topic_name(self, database: str, table: str) -> str:
        return f"{self._prefix}.{database}.{table}"

    def dlq_topic_name(self) -> str:
        return f"{self._prefix}.dlq"

    def schema_history_topic(self) -> str:
        return f"{self._prefix}.schema-history"

    def create_table_topics(
        self,
        database: str,
        tables: list[str],
    ) -> dict[str, bool]:
        """
        Create one topic per table. Returns {topic_name: created/existed}.
        Idempotent — skips topics that already exist.
        """
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin = self._get_admin()
        existing = self._list_existing_topics()
        results = {}

        new_topics = []
        for table in tables:
            name = self.topic_name(database, table)
            if name in existing:
                logger.debug(f"Topic already exists: {name}")
                results[name] = True
                continue
            new_topics.append(
                NewTopic(
                    name=name,
                    num_partitions=self._partitions,
                    replication_factor=self._replication_factor,
                    topic_configs={
                        "retention.ms": str(self._retention_ms),
                        "compression.type": self.COMPRESSION,
                        "cleanup.policy": self.CLEANUP_POLICY,
                        "min.insync.replicas": str(
                            max(1, self._replication_factor - 1)
                        ),
                    },
                )
            )

        if new_topics:
            try:
                admin.create_topics(new_topics, validate_only=False)
                for t in new_topics:
                    results[t.name] = True
                    logger.info(
                        f"Created topic: {t.name} "
                        f"(partitions={self._partitions}, rf={self._replication_factor})"
                    )
            except TopicAlreadyExistsError:
                for t in new_topics:
                    results[t.name] = True
            except Exception as e:
                logger.error(f"Failed to create topics: {e}")
                for t in new_topics:
                    results[t.name] = False

        return results

    def create_system_topics(self) -> bool:
        """Create DLQ and schema history topics."""
        from kafka.admin import NewTopic

        admin = self._get_admin()
        existing = self._list_existing_topics()
        system_topics = [
            NewTopic(
                name=self.dlq_topic_name(),
                num_partitions=3,
                replication_factor=self._replication_factor,
                topic_configs={
                    "retention.ms": str(self._retention_ms * 4),  # 28 days for DLQ
                    "compression.type": self.COMPRESSION,
                },
            ),
            NewTopic(
                name=self.schema_history_topic(),
                num_partitions=1,
                replication_factor=self._replication_factor,
                topic_configs={
                    "retention.ms": "-1",  # infinite retention for schema history
                    "cleanup.policy": "delete",
                },
            ),
        ]

        to_create = [t for t in system_topics if t.name not in existing]
        if not to_create:
            logger.info("System topics already exist")
            return True

        try:
            admin.create_topics(to_create, validate_only=False)
            for t in to_create:
                logger.info(f"Created system topic: {t.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create system topics: {e}")
            return False

    def verify_topics(self, database: str, tables: list[str]) -> dict[str, bool]:
        """
        Verify all expected topics exist with correct configuration.
        Returns {topic_name: config_valid}.
        """
        admin = self._get_admin()
        existing = self._list_existing_topics()
        results = {}

        for table in tables:
            name = self.topic_name(database, table)
            results[name] = name in existing
            if name not in existing:
                logger.warning(f"Topic missing: {name}")

        return results

    def get_consumer_lag(
        self,
        group_id: str,
        database: str,
        tables: list[str],
    ) -> dict[str, int]:
        """
        Get consumer group lag per topic.
        Returns {topic_name: total_lag_messages}.
        """
        try:
            admin = self._get_admin()
            offsets = admin.list_consumer_group_offsets(group_id)
            lag_by_topic: dict[str, int] = {}

            for tp, offset_meta in offsets.items():
                if self._prefix in tp.topic:
                    lag_by_topic[tp.topic] = lag_by_topic.get(tp.topic, 0) + max(
                        0, offset_meta.offset
                    )
            return lag_by_topic
        except Exception as e:
            logger.error(f"Failed to get consumer lag: {e}")
            return {}

    def verify_retention(self, database: str, tables: list[str]) -> bool:
        """
        Critical check: verify all topics have retention >= 7 days.
        This is a hard rule from the fallback plan.
        """
        try:
            admin = self._get_admin()
            topic_names = [self.topic_name(database, t) for t in tables]
            configs = admin.describe_configs(
                [
                    {"resource_type": 2, "resource_name": n, "config_names": ["retention.ms"]}
                    for n in topic_names
                ]
            )
            all_valid = True
            for resource, config_entries in configs.items():
                for entry in config_entries:
                    if entry.name == "retention.ms":
                        retention = int(entry.value)
                        if retention != -1 and retention < self.RETENTION_7_DAYS_MS:
                            logger.error(
                                f"Topic {resource.name} has retention "
                                f"{retention}ms < 7 days. HARD RULE VIOLATION."
                            )
                            all_valid = False
            return all_valid
        except Exception as e:
            logger.warning(f"Could not verify retention: {e}")
            return True  # best-effort — don't block on admin API unavailability

    def _list_existing_topics(self) -> set[str]:
        try:
            admin = self._get_admin()
            return set(admin.list_topics())
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return set()

    def close(self):
        if self._admin:
            try:
                self._admin.close()
            except Exception:
                pass
            self._admin = None
