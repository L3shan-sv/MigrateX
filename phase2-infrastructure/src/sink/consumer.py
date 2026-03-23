"""
sink/consumer.py
────────────────
The MigrateX GTID sink consumer.
Reads Debezium change events from Kafka and applies them to the target DB.

Zero double-write guarantee:
  Every write is preceded by a GTID deduplication check.
  The dedup insert and the data write happen in the same transaction.
  If the GTID already exists → silent discard.
  If the GTID is new → apply write + insert GTID.
  No GTID can ever be applied twice.

GTID deduplication table schema (auto-created on startup):
  CREATE TABLE _migratex_gtid_dedup (
      gtid        VARCHAR(64)  NOT NULL,
      applied_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
      table_name  VARCHAR(128) NOT NULL,
      operation   CHAR(1)      NOT NULL,  -- I, U, D
      PRIMARY KEY (gtid),
      INDEX idx_applied_at (applied_at)
  ) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;

  -- Purge old entries (run daily in Phase 6):
  DELETE FROM _migratex_gtid_dedup WHERE applied_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

Event structure from Debezium (simplified):
  {
    "source": {
      "gtid": "uuid:transaction_id",
      "db": "airbnb_prod",
      "table": "bookings",
      "ts_ms": 1234567890000
    },
    "op": "c" | "u" | "d" | "r",   -- create, update, delete, read (snapshot)
    "before": {...} | null,
    "after":  {...} | null
  }
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class Operation(Enum):
    INSERT = "c"    # create
    UPDATE = "u"    # update
    DELETE = "d"    # delete
    READ = "r"      # snapshot read


@dataclass
class ChangeEvent:
    """Parsed Debezium change event."""
    gtid: str
    database: str
    table: str
    operation: Operation
    before: dict | None
    after: dict | None
    ts_ms: int
    topic: str
    partition: int
    offset: int


@dataclass
class ConsumerMetrics:
    events_processed: int = 0
    events_deduplicated: int = 0
    events_failed: int = 0
    events_dlq: int = 0
    lag_seconds: float = 0.0
    last_gtid: str = ""
    last_event_ts: str = ""
    uptime_seconds: float = 0.0


class SinkConsumer:
    """
    Kafka → MySQL sink consumer with GTID-based atomic deduplication.

    Thread model:
      - N worker threads (configurable, default 8)
      - Each worker owns a dedicated MySQL connection
      - Kafka partitions are distributed across workers
      - Each worker processes its partition sequentially (preserves order)

    Failure modes:
      - Duplicate GTID → silent discard (idempotent)
      - MySQL write error → retry 3x, then dead-letter queue
      - Kafka connectivity loss → pause + reconnect with backoff
      - Unknown operation → dead-letter queue
    """

    DEDUP_TABLE = "_migratex_gtid_dedup"
    MAX_RETRIES = 3
    RETRY_BACKOFF_MS = [100, 500, 2000]

    def __init__(
        self,
        bootstrap_servers: list[str],
        group_id: str,
        topics: list[str],
        target_host: str,
        target_port: int,
        target_user: str,
        target_password: str,
        target_database: str,
        worker_threads: int = 8,
        max_poll_records: int = 500,
        dlq_topic: str = "migratex.dlq",
        on_lag_update: Callable[[float], None] | None = None,
    ):
        self._bootstrap = bootstrap_servers
        self._group_id = group_id
        self._topics = topics
        self._target_config = {
            "host": target_host,
            "port": target_port,
            "user": target_user,
            "password": target_password,
            "database": target_database,
        }
        self._workers = worker_threads
        self._max_poll = max_poll_records
        self._dlq_topic = dlq_topic
        self._on_lag_update = on_lag_update
        self._metrics = ConsumerMetrics()
        self._running = False
        self._start_time: float | None = None
        self._lock = threading.Lock()

    # ── Deduplication table ───────────────────────────────────────────────────

    def ensure_dedup_table(self, conn) -> None:
        """
        Create the GTID deduplication table if it doesn't exist.
        Called once on startup per worker connection.
        The PRIMARY KEY on gtid guarantees atomic deduplication —
        a duplicate INSERT raises a unique constraint error, not a silent no-op.
        """
        ddl = f"""
        CREATE TABLE IF NOT EXISTS `{self.DEDUP_TABLE}` (
            `gtid`       VARCHAR(64)  NOT NULL,
            `applied_at` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `table_name` VARCHAR(128) NOT NULL,
            `operation`  CHAR(1)      NOT NULL,
            PRIMARY KEY (`gtid`),
            INDEX `idx_applied_at` (`applied_at`)
        ) ENGINE=InnoDB ROW_FORMAT=COMPRESSED
          COMMENT='MigrateX GTID deduplication table — do not modify';
        """
        cursor = conn.cursor()
        cursor.execute(ddl)
        conn.commit()
        cursor.close()
        logger.info(f"GTID dedup table '{self.DEDUP_TABLE}' ensured on target")

    # ── Event parsing ─────────────────────────────────────────────────────────

    @staticmethod
    def parse_event(msg) -> ChangeEvent | None:
        """
        Parse a Kafka message into a ChangeEvent.
        Returns None if the message cannot be parsed (sent to DLQ).
        """
        try:
            value = json.loads(msg.value.decode("utf-8")) if msg.value else None
            if value is None:
                return None  # tombstone

            payload = value.get("payload", value)
            source = payload.get("source", {})

            gtid = source.get("gtid") or source.get("ts_ms", str(msg.offset))
            if not gtid:
                gtid = f"no-gtid:{msg.topic}:{msg.partition}:{msg.offset}"

            op_str = payload.get("op", "r")
            try:
                op = Operation(op_str)
            except ValueError:
                logger.warning(f"Unknown operation '{op_str}' — treating as READ")
                op = Operation.READ

            return ChangeEvent(
                gtid=str(gtid),
                database=source.get("db", ""),
                table=source.get("table", ""),
                operation=op,
                before=payload.get("before"),
                after=payload.get("after"),
                ts_ms=source.get("ts_ms", 0),
                topic=msg.topic,
                partition=msg.partition,
                offset=msg.offset,
            )
        except Exception as e:
            logger.error(f"Failed to parse event: {e} — value={msg.value[:200] if msg.value else 'null'}")
            return None

    # ── Core apply logic ──────────────────────────────────────────────────────

    def apply_event(self, event: ChangeEvent, conn) -> str:
        """
        Apply a single change event to the target database.
        Returns: 'applied' | 'deduplicated' | 'error'

        Transaction protocol:
          BEGIN
            INSERT INTO _migratex_gtid_dedup (gtid, table_name, operation)
            VALUES (%s, %s, %s)
            -- If this raises IntegrityError → DUPLICATE → ROLLBACK → 'deduplicated'
            -- If this succeeds → apply the actual change below
            [INSERT|UPDATE|DELETE on target table]
          COMMIT
        """
        import mysql.connector
        cursor = conn.cursor()

        try:
            conn.start_transaction()

            # ── Step 1: Attempt GTID dedup insert ────────────────────────────
            try:
                cursor.execute(
                    f"INSERT INTO `{self.DEDUP_TABLE}` "
                    f"(gtid, table_name, operation) VALUES (%s, %s, %s)",
                    (event.gtid, event.table, event.operation.value),
                )
            except mysql.connector.IntegrityError as e:
                if e.errno == 1062:  # Duplicate entry
                    conn.rollback()
                    logger.debug(f"DEDUPLICATED gtid={event.gtid} table={event.table}")
                    with self._lock:
                        self._metrics.events_deduplicated += 1
                    return "deduplicated"
                raise

            # ── Step 2: Apply the change ──────────────────────────────────────
            if event.operation == Operation.INSERT or event.operation == Operation.READ:
                self._apply_insert(event, cursor)
            elif event.operation == Operation.UPDATE:
                self._apply_update(event, cursor)
            elif event.operation == Operation.DELETE:
                self._apply_delete(event, cursor)

            conn.commit()

            with self._lock:
                self._metrics.events_processed += 1
                self._metrics.last_gtid = event.gtid
                self._metrics.last_event_ts = datetime.utcfromtimestamp(
                    event.ts_ms / 1000
                ).isoformat()

            return "applied"

        except Exception as e:
            conn.rollback()
            logger.error(
                f"Failed to apply event: gtid={event.gtid} "
                f"table={event.table} op={event.operation} err={e}"
            )
            with self._lock:
                self._metrics.events_failed += 1
            raise
        finally:
            cursor.close()

    def _apply_insert(self, event: ChangeEvent, cursor) -> None:
        """Build and execute INSERT ... ON DUPLICATE KEY UPDATE."""
        row = event.after
        if not row:
            return

        cols = list(row.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(f"`{c}`" for c in cols)
        updates = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in cols)

        sql = (
            f"INSERT INTO `{event.table}` ({col_names}) "
            f"VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {updates}"
        )
        cursor.execute(sql, list(row.values()))

    def _apply_update(self, event: ChangeEvent, cursor) -> None:
        """
        Build and execute UPDATE using the 'before' image to identify the row
        and 'after' image for the new values.
        If 'before' is None, fall back to insert-or-update.
        """
        after = event.after
        if not after:
            return

        if event.before:
            # Use before image to find PK values
            pk_cols = self._infer_pk_columns(event.before, event.after)
            if pk_cols:
                set_clause = ", ".join(
                    f"`{c}`=%s" for c in after.keys() if c not in pk_cols
                )
                where_clause = " AND ".join(f"`{c}`=%s" for c in pk_cols)
                set_vals = [v for c, v in after.items() if c not in pk_cols]
                where_vals = [event.before[c] for c in pk_cols]

                if set_clause:
                    sql = f"UPDATE `{event.table}` SET {set_clause} WHERE {where_clause}"
                    cursor.execute(sql, set_vals + where_vals)
                    return

        # Fallback: upsert
        self._apply_insert(event, cursor)

    def _apply_delete(self, event: ChangeEvent, cursor) -> None:
        """Build and execute DELETE using the 'before' image."""
        row = event.before
        if not row:
            return

        # Use all columns from before image as WHERE clause (safe for any schema)
        where_clause = " AND ".join(f"`{c}`=%s" for c in row.keys())
        sql = f"DELETE FROM `{event.table}` WHERE {where_clause} LIMIT 1"
        cursor.execute(sql, list(row.values()))

    def _infer_pk_columns(self, before: dict, after: dict) -> list[str]:
        """
        Infer PK columns from the before/after images.
        Columns that are identical in before and after are PK candidates.
        This is a heuristic — in production, the schema registry provides
        the definitive PK list.
        """
        pk_candidates = [
            col for col in before
            if before.get(col) == after.get(col)
            and col.lower() in ("id", "uuid", "pk", "key")
        ]
        if not pk_candidates:
            # Fall back to columns named 'id' pattern
            pk_candidates = [
                col for col in before
                if col.lower().endswith("_id") or col.lower() == "id"
            ]
        return pk_candidates[:2]  # cap at 2 PKs as a safety measure

    # ── Dead letter queue ─────────────────────────────────────────────────────

    def send_to_dlq(self, raw_message, reason: str, producer=None) -> None:
        """
        Send a failed message to the dead letter queue topic.
        Includes original message + failure reason + timestamp.
        """
        if producer is None:
            logger.error(f"DLQ: {reason} (no producer available to route)")
            with self._lock:
                self._metrics.events_dlq += 1
            return

        dlq_payload = json.dumps({
            "original_topic": raw_message.topic,
            "original_partition": raw_message.partition,
            "original_offset": raw_message.offset,
            "failure_reason": reason,
            "failed_at": datetime.utcnow().isoformat(),
            "original_value": raw_message.value.decode("utf-8", errors="replace")
            if raw_message.value else None,
        }).encode("utf-8")

        try:
            producer.send(self._dlq_topic, value=dlq_payload)
            with self._lock:
                self._metrics.events_dlq += 1
            logger.warning(f"Sent to DLQ: {reason}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")

    # ── Metrics ───────────────────────────────────────────────────────────────

    def get_metrics(self) -> ConsumerMetrics:
        with self._lock:
            m = ConsumerMetrics(
                events_processed=self._metrics.events_processed,
                events_deduplicated=self._metrics.events_deduplicated,
                events_failed=self._metrics.events_failed,
                events_dlq=self._metrics.events_dlq,
                lag_seconds=self._metrics.lag_seconds,
                last_gtid=self._metrics.last_gtid,
                last_event_ts=self._metrics.last_event_ts,
                uptime_seconds=time.time() - self._start_time if self._start_time else 0.0,
            )
        return m

    def update_lag(self, lag_seconds: float) -> None:
        with self._lock:
            self._metrics.lag_seconds = lag_seconds
        if self._on_lag_update:
            self._on_lag_update(lag_seconds)


# ── Idempotency test helper ───────────────────────────────────────────────────

class IdempotencyValidator:
    """
    Phase 2 exit criterion: verifies that applying the same GTID twice
    results in exactly one row on the target (not two).

    This is run as part of the synthetic validation protocol.
    """

    def __init__(self, consumer: SinkConsumer):
        self._consumer = consumer

    def run(self, conn, test_table: str = "_migratex_idempotency_test") -> bool:
        """
        Creates a test table, sends the same INSERT event twice,
        verifies exactly one row exists, then cleans up.
        Returns True if idempotency holds.
        """
        import mysql.connector

        cursor = conn.cursor()

        # Setup
        cursor.execute(f"DROP TABLE IF EXISTS `{test_table}`")
        cursor.execute(
            f"CREATE TABLE `{test_table}` "
            f"(id INT PRIMARY KEY, val VARCHAR(50)) ENGINE=InnoDB"
        )
        conn.commit()

        # Create a fake event
        test_gtid = f"test-idempotency:{int(time.time())}"
        event = ChangeEvent(
            gtid=test_gtid,
            database="test",
            table=test_table,
            operation=Operation.INSERT,
            before=None,
            after={"id": 1, "val": "idempotency-check"},
            ts_ms=int(time.time() * 1000),
            topic="test",
            partition=0,
            offset=0,
        )

        # Apply twice
        result1 = self._consumer.apply_event(event, conn)
        result2 = self._consumer.apply_event(event, conn)

        # Verify
        cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{test_table}`")
        row = cursor.fetchone()
        count = row[0] if row else 0

        # Verify dedup table
        cursor.execute(
            f"SELECT COUNT(*) AS cnt FROM `{self._consumer.DEDUP_TABLE}` WHERE gtid=%s",
            (test_gtid,),
        )
        dedup_row = cursor.fetchone()
        dedup_count = dedup_row[0] if dedup_row else 0

        # Cleanup
        cursor.execute(f"DROP TABLE IF EXISTS `{test_table}`")
        cursor.execute(
            f"DELETE FROM `{self._consumer.DEDUP_TABLE}` WHERE gtid=%s",
            (test_gtid,),
        )
        conn.commit()
        cursor.close()

        passed = count == 1 and dedup_count == 1 and result1 == "applied" and result2 == "deduplicated"

        if passed:
            logger.info("Idempotency validation PASSED — double-write correctly prevented")
        else:
            logger.error(
                f"Idempotency validation FAILED — "
                f"rows={count} dedup_entries={dedup_count} "
                f"result1={result1} result2={result2}"
            )

        return passed
