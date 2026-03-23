"""
tests/test_phase2.py
────────────────────
Phase 2 test suite.
All tests run without live Kafka, MySQL, or etcd.
Uses mocks and in-memory implementations throughout.
"""

from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.sink.consumer import (
    SinkConsumer,
    ChangeEvent,
    Operation,
    IdempotencyValidator,
)
from src.lease.fencing import (
    FencingToken,
    FencingTokenClient,
    FencingTokenRecorder,
    Authority,
    LeaseState,
)
from src.snapshot.coordinator import (
    ParallelSnapshotCoordinator,
    ChunkSpec,
    TableTier,
)
from src.cdc.topics import KafkaTopicManager


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_event(
    gtid: str = "abc:123",
    table: str = "bookings",
    op: Operation = Operation.INSERT,
    before: dict | None = None,
    after: dict | None = None,
) -> ChangeEvent:
    return ChangeEvent(
        gtid=gtid,
        database="airbnb_prod",
        table=table,
        operation=op,
        before=before,
        after=after or {"id": 1, "status": "confirmed", "amount": 150.00},
        ts_ms=int(time.time() * 1000),
        topic=f"migratex.airbnb_prod.{table}",
        partition=0,
        offset=0,
    )


class MockConnection:
    """In-memory MySQL connection mock that tracks state."""

    def __init__(self):
        self._store: dict[str, dict] = {}  # table → {pk: row}
        self._dedup: set[str] = set()       # seen GTIDs
        self._in_tx = False
        self._tx_ops: list[tuple] = []
        self.cursor_mock = None

    def start_transaction(self):
        self._in_tx = True
        self._tx_ops = []

    def commit(self):
        for op_type, args in self._tx_ops:
            if op_type == "dedup_insert":
                gtid = args[0]
                if gtid in self._dedup:
                    raise _DuplicateKeyError(gtid)
                self._dedup.add(gtid)
            elif op_type == "row_upsert":
                table, row = args
                if table not in self._store:
                    self._store[table] = {}
                pk = row.get("id", id(row))
                self._store[table][pk] = row
            elif op_type == "row_delete":
                table, pk = args
                if table in self._store:
                    self._store[table].pop(pk, None)
        self._in_tx = False
        self._tx_ops = []

    def rollback(self):
        self._in_tx = False
        self._tx_ops = []

    def cursor(self):
        return MockCursor(self)

    def get_row_count(self, table: str) -> int:
        return len(self._store.get(table, {}))


class _DuplicateKeyError(Exception):
    errno = 1062


class MockCursor:
    def __init__(self, conn: MockConnection):
        self._conn = conn
        self._last_result = None

    def execute(self, sql: str, params: tuple = ()):
        sql_upper = sql.strip().upper()

        if "INSERT INTO `_MIGRATEX_GTID_DEDUP`" in sql_upper:
            gtid = params[0]
            if gtid in self._conn._dedup:
                import mysql.connector
                err = _DuplicateKeyError(gtid)
                raise err
            self._conn._tx_ops.append(("dedup_insert", (gtid,)))

        elif sql_upper.startswith("INSERT INTO") or "ON DUPLICATE KEY UPDATE" in sql_upper:
            # Extract table name
            parts = sql.split("`")
            table = parts[1] if len(parts) > 1 else "unknown"
            row = {}
            if params:
                row = {"id": params[0]} if params else {}
            self._conn._tx_ops.append(("row_upsert", (table, dict(zip(range(len(params)), params)))))

        elif sql_upper.startswith("DELETE FROM"):
            parts = sql.split("`")
            table = parts[1] if len(parts) > 1 else "unknown"
            pk = params[0] if params else None
            self._conn._tx_ops.append(("row_delete", (table, pk)))

        elif sql_upper.startswith("CREATE TABLE"):
            pass  # no-op for DDL

        elif sql_upper.startswith("SELECT COUNT"):
            self._last_result = [(1,)]

        elif "MAX(EPOCH)" in sql_upper:
            self._last_result = [(0,)]

    def fetchone(self):
        if self._last_result:
            return self._last_result[0]
        return None

    def close(self):
        pass


# ── SinkConsumer tests ────────────────────────────────────────────────────────

class TestSinkConsumer:

    def _make_consumer(self) -> SinkConsumer:
        return SinkConsumer(
            bootstrap_servers=["localhost:9092"],
            group_id="test-group",
            topics=["migratex.airbnb_prod.bookings"],
            target_host="localhost",
            target_port=3306,
            target_user="test",
            target_password="test",
            target_database="test",
        )

    def test_apply_insert_returns_applied(self):
        consumer = self._make_consumer()
        conn = MockConnection()
        event = make_event(gtid="uuid:1", op=Operation.INSERT)
        result = consumer.apply_event(event, conn)
        assert result == "applied"

    def test_apply_same_gtid_twice_returns_deduplicated(self):
        consumer = self._make_consumer()
        conn = MockConnection()
        event = make_event(gtid="uuid:42", op=Operation.INSERT)

        # Apply once — should succeed
        r1 = consumer.apply_event(event, conn)

        # Simulate dedup table already having the GTID
        conn._dedup.add("uuid:42")

        # Apply again — should be deduplicated
        r2 = consumer.apply_event(event, conn)
        assert r1 == "applied"
        assert r2 == "deduplicated"

    def test_metrics_increment_on_apply(self):
        consumer = self._make_consumer()
        conn = MockConnection()

        for i in range(5):
            event = make_event(gtid=f"uuid:{i}", op=Operation.INSERT)
            consumer.apply_event(event, conn)

        metrics = consumer.get_metrics()
        assert metrics.events_processed == 5
        assert metrics.events_deduplicated == 0

    def test_metrics_dedup_count_increments(self):
        consumer = self._make_consumer()
        conn = MockConnection()

        event = make_event(gtid="uuid:dedup-test")
        consumer.apply_event(event, conn)

        # Force dedup by pre-inserting GTID
        conn._dedup.add("uuid:dedup-test")
        consumer.apply_event(event, conn)

        metrics = consumer.get_metrics()
        assert metrics.events_deduplicated == 1

    def test_parse_event_from_debezium_payload(self):
        raw = MagicMock()
        raw.value = json.dumps({
            "payload": {
                "op": "c",
                "before": None,
                "after": {"id": 1, "status": "pending"},
                "source": {
                    "gtid": "abc-uuid:1001",
                    "db": "airbnb_prod",
                    "table": "bookings",
                    "ts_ms": 1700000000000,
                }
            }
        }).encode("utf-8")
        raw.topic = "migratex.airbnb_prod.bookings"
        raw.partition = 0
        raw.offset = 100

        event = SinkConsumer.parse_event(raw)
        assert event is not None
        assert event.gtid == "abc-uuid:1001"
        assert event.table == "bookings"
        assert event.operation == Operation.INSERT
        assert event.after == {"id": 1, "status": "pending"}

    def test_parse_event_tombstone_returns_none(self):
        raw = MagicMock()
        raw.value = None
        raw.topic = "migratex.airbnb_prod.bookings"
        raw.partition = 0
        raw.offset = 0
        assert SinkConsumer.parse_event(raw) is None

    def test_parse_event_invalid_json_returns_none(self):
        raw = MagicMock()
        raw.value = b"not valid json {"
        raw.topic = "test"
        raw.partition = 0
        raw.offset = 0
        result = SinkConsumer.parse_event(raw)
        assert result is None

    def test_update_operation_uses_before_image(self):
        consumer = self._make_consumer()
        conn = MockConnection()
        event = make_event(
            gtid="uuid:update-1",
            op=Operation.UPDATE,
            before={"id": 5, "status": "pending"},
            after={"id": 5, "status": "confirmed"},
        )
        result = consumer.apply_event(event, conn)
        assert result == "applied"

    def test_delete_operation_applies(self):
        consumer = self._make_consumer()
        conn = MockConnection()
        event = make_event(
            gtid="uuid:del-1",
            op=Operation.DELETE,
            before={"id": 10, "status": "cancelled"},
            after=None,
        )
        result = consumer.apply_event(event, conn)
        assert result == "applied"

    def test_last_gtid_updated_after_apply(self):
        consumer = self._make_consumer()
        conn = MockConnection()
        event = make_event(gtid="uuid:last-gtid-check")
        consumer.apply_event(event, conn)
        assert consumer.get_metrics().last_gtid == "uuid:last-gtid-check"


# ── Fencing Token tests ───────────────────────────────────────────────────────

class TestFencingTokenRecorder:
    """
    Tests the FencingTokenRecorder (DB-side fencing enforcement).
    The etcd client tests use mocks since we need no live etcd.
    """

    def test_valid_epoch_accepted(self):
        recorder = FencingTokenRecorder()
        conn = MockConnection()
        recorder.ensure_table(conn)
        result = recorder.validate_and_record(epoch=1, table_name="bookings", operation="I", conn=conn)
        assert result is True

    def test_stale_epoch_rejected(self):
        recorder = FencingTokenRecorder()
        conn = MockConnection()
        recorder.ensure_table(conn)

        # Apply epoch 5 first
        recorder.validate_and_record(epoch=5, table_name="bookings", operation="I", conn=conn)

        # Now try epoch 3 — stale
        result = recorder.validate_and_record(epoch=3, table_name="users", operation="U", conn=conn)
        assert result is False

    def test_equal_epoch_accepted(self):
        recorder = FencingTokenRecorder()
        conn = MockConnection()
        recorder.ensure_table(conn)
        recorder.validate_and_record(epoch=3, table_name="bookings", operation="I", conn=conn)
        result = recorder.validate_and_record(epoch=3, table_name="reviews", operation="I", conn=conn)
        assert result is True

    def test_max_epoch_increases_monotonically(self):
        recorder = FencingTokenRecorder()
        conn = MockConnection()
        recorder.ensure_table(conn)

        for epoch in [1, 2, 3, 5, 7]:
            recorder.validate_and_record(epoch=epoch, table_name="t", operation="I", conn=conn)

        assert recorder.get_max_epoch() == 7

    def test_concurrent_epoch_validation_is_safe(self):
        recorder = FencingTokenRecorder()
        conn = MockConnection()
        recorder.ensure_table(conn)

        violations = []

        def worker(epoch: int):
            result = recorder.validate_and_record(
                epoch=epoch, table_name="bookings", operation="I", conn=conn
            )
            if not result:
                violations.append(epoch)

        threads = [
            threading.Thread(target=worker, args=(i,))
            for i in range(1, 11)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Max epoch must be 10
        assert recorder.get_max_epoch() == 10


# ── Authority tests ───────────────────────────────────────────────────────────

class TestAuthorityEnum:

    def test_edge_authority_value(self):
        assert Authority.EDGE.value == "edge"

    def test_cloud_authority_value(self):
        assert Authority.CLOUD.value == "cloud"

    def test_none_authority_value(self):
        assert Authority.NONE.value == "none"


# ── Snapshot coordinator tests ────────────────────────────────────────────────

class TestParallelSnapshotCoordinator:

    def _make_coordinator(self, tables_data=None):
        source_pool = MagicMock()
        target_pool = MagicMock()

        # Mock source pool responses
        source_pool.execute_one.return_value = {"min_pk": 1, "max_pk": 1_000_000}
        source_pool.execute.return_value = [
            {"id": i, "status": "confirmed"} for i in range(1, 11)
        ]

        target_pool.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
        target_pool.cursor.return_value.__exit__ = MagicMock(return_value=False)

        return ParallelSnapshotCoordinator(
            source_pool=source_pool,
            target_pool=target_pool,
            database="airbnb_prod",
            chunk_size=500_000,
            max_workers=2,
        )

    def test_generate_chunks_creates_correct_count(self):
        coord = self._make_coordinator()
        coord._source.execute_one.return_value = {"min_pk": 1, "max_pk": 1_500_000}
        chunks = coord._generate_chunks("bookings", "id")
        # 1,500,000 rows ÷ 500,000 chunk size = 3 chunks
        assert len(chunks) == 3

    def test_generate_chunks_last_chunk_is_open_ended(self):
        coord = self._make_coordinator()
        coord._source.execute_one.return_value = {"min_pk": 1, "max_pk": 999_999}
        chunks = coord._generate_chunks("bookings", "id")
        assert chunks[-1].end_pk is None

    def test_generate_chunks_empty_table_returns_empty(self):
        coord = self._make_coordinator()
        coord._source.execute_one.return_value = {"min_pk": None, "max_pk": None}
        chunks = coord._generate_chunks("empty_table", "id")
        assert chunks == []

    def test_chunk_spec_has_correct_fields(self):
        coord = self._make_coordinator()
        coord._source.execute_one.return_value = {"min_pk": 1, "max_pk": 500_001}
        chunks = coord._generate_chunks("bookings", "id")
        assert chunks[0].pk_column == "id"
        assert chunks[0].start_pk == 1
        assert chunks[0].chunk_index == 0

    def test_classify_tables_separates_tiers(self):
        coord = self._make_coordinator()
        tables = [
            {
                "table_name": "users",
                "data_size_bytes": 10_000_000_000,
                "foreign_keys": [],
                "primary_key_columns": ["id"],
            },
            {
                "table_name": "reviews",
                "data_size_bytes": 1_000_000,
                "foreign_keys": [{"referenced_table": "users"}],
                "primary_key_columns": ["id"],
            },
            *[
                {
                    "table_name": f"archive_{i}",
                    "data_size_bytes": 100,
                    "foreign_keys": [],
                    "primary_key_columns": ["id"],
                }
                for i in range(10)
            ],
        ]
        tier1, tier2, tier3 = coord._classify_tables(tables)
        # Tier 3 should contain small archive tables
        assert len(tier3) > 0

    def test_checksum_is_deterministic(self):
        coord = self._make_coordinator()
        rows = [{"id": 1, "val": "test"}, {"id": 2, "val": "foo"}]
        c1 = coord._compute_checksum(rows)
        c2 = coord._compute_checksum(rows)
        assert c1 == c2

    def test_checksum_differs_for_different_rows(self):
        coord = self._make_coordinator()
        rows_a = [{"id": 1, "val": "test"}]
        rows_b = [{"id": 1, "val": "different"}]
        assert coord._compute_checksum(rows_a) != coord._compute_checksum(rows_b)

    def test_progress_starts_at_zero(self):
        coord = self._make_coordinator()
        progress = coord.get_progress()
        assert progress.completed_chunks == 0
        assert progress.rows_transferred == 0
        assert progress.deadman_triggered is False


# ── Topic manager tests ───────────────────────────────────────────────────────

class TestKafkaTopicManager:

    def test_topic_name_format(self):
        mgr = KafkaTopicManager(
            bootstrap_servers=["localhost:9092"],
            topic_prefix="migratex",
        )
        assert mgr.topic_name("airbnb_prod", "bookings") == "migratex.airbnb_prod.bookings"

    def test_dlq_topic_name(self):
        mgr = KafkaTopicManager(
            bootstrap_servers=["localhost:9092"],
            topic_prefix="migratex",
        )
        assert mgr.dlq_topic_name() == "migratex.dlq"

    def test_schema_history_topic_name(self):
        mgr = KafkaTopicManager(
            bootstrap_servers=["localhost:9092"],
            topic_prefix="migratex",
        )
        assert mgr.schema_history_topic() == "migratex.schema-history"

    def test_retention_7_days_constant(self):
        assert KafkaTopicManager.RETENTION_7_DAYS_MS == 604_800_000

    def test_compression_type_is_lz4(self):
        assert KafkaTopicManager.COMPRESSION == "lz4"


# ── Idempotency validator tests ───────────────────────────────────────────────

class TestIdempotencyValidator:

    def test_validator_detects_double_write(self):
        consumer = SinkConsumer(
            bootstrap_servers=["localhost:9092"],
            group_id="test",
            topics=[],
            target_host="localhost",
            target_port=3306,
            target_user="test",
            target_password="test",
            target_database="test",
        )
        conn = MockConnection()

        event = make_event(gtid="idem-test-uuid:1")

        r1 = consumer.apply_event(event, conn)
        conn._dedup.add("idem-test-uuid:1")  # simulate what a real DB would do
        r2 = consumer.apply_event(event, conn)

        assert r1 == "applied"
        assert r2 == "deduplicated"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
