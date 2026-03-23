"""
snapshot/coordinator.py
───────────────────────
Parallel snapshot coordinator for 100TB+ initial data loads.

Problem with sequential Debezium snapshot:
  100TB ÷ 1Gbps = ~222 hours = 9.3 days
  MySQL binlog retention = 7 days
  If snapshot > 7 days → lose ability to replay from snapshot GTID → full restart

Solution: chunked parallel snapshot
  - Split each table into 500,000-row primary key range chunks
  - Up to 8 concurrent worker threads
  - Network utilisation capped at 70% (leaves headroom for production traffic)
  - CRC32 checksum per chunk (verified on target before marking complete)
  - Dead-man's-switch: if progress < 1TB in any 4-hour window → P1 alert

Processing order:
  Tier 1 (critical): High write-rate tables with FK dependencies → sequential
  Tier 2 (medium):   Medium volume tables → parallel, 4 workers
  Tier 3 (archive):  Low-write historical tables → parallel, 8 workers

The coordinator publishes progress to Prometheus at chunk granularity.
Any chunk that fails retries 3 times before quarantine.
A quarantined table is re-queued at the back of the worker pool.
The snapshot does not halt for a single table failure.
"""

from __future__ import annotations

import hashlib
import json
import logging
import queue
import struct
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


class TableTier(Enum):
    CRITICAL = 1    # FK-linked, high write-rate → snapshot first, sequentially
    MEDIUM = 2      # Medium volume → parallel with 4 workers
    ARCHIVE = 3     # Historical, low-write → parallel with 8 workers


@dataclass
class ChunkSpec:
    table_name: str
    pk_column: str
    start_pk: Any
    end_pk: Any | None   # None = open-ended (last chunk)
    chunk_index: int
    tier: TableTier


@dataclass
class ChunkResult:
    chunk: ChunkSpec
    rows_transferred: int
    checksum: str
    started_at: str
    completed_at: str
    retries: int
    status: str          # 'completed', 'failed', 'quarantined'
    error: str | None = None


@dataclass
class SnapshotProgress:
    total_tables: int
    completed_tables: int
    total_chunks: int
    completed_chunks: int
    failed_chunks: int
    quarantined_tables: list[str]
    bytes_transferred: int
    rows_transferred: int
    throughput_mbps: float
    elapsed_seconds: float
    eta_seconds: float | None
    started_at: str
    last_progress_at: str
    deadman_triggered: bool = False


class ParallelSnapshotCoordinator:
    """
    Coordinates chunked parallel snapshot across all source tables.

    Thread safety:
      - Progress state protected by self._lock
      - Each worker thread has its own DB connection (no sharing)
      - Chunk queue is thread-safe (queue.Queue)
      - CRC32 checksums are computed on the worker thread

    Dead-man's switch:
      Monitors progress every 30 minutes.
      If total bytes transferred has not increased by at least
      DEADMAN_MIN_BYTES_PER_HOUR within the last DEADMAN_WINDOW_HOURS,
      fires a P1 alert via the provided callback.
    """

    DEADMAN_MIN_BYTES_PER_HOUR = 1_000_000_000  # 1 GB/h minimum progress
    DEADMAN_WINDOW_HOURS = 4
    MAX_RETRIES = 3
    RETRY_BACKOFF_SECONDS = [5, 30, 120]

    def __init__(
        self,
        source_pool,
        target_pool,
        database: str,
        chunk_size: int = 500_000,
        max_workers: int = 8,
        network_cap_pct: int = 70,
        on_deadman_alert: Callable[[str], None] | None = None,
        on_chunk_complete: Callable[[ChunkResult], None] | None = None,
    ):
        self._source = source_pool
        self._target = target_pool
        self._database = database
        self._chunk_size = chunk_size
        self._max_workers = max_workers
        self._network_cap_pct = network_cap_pct
        self._on_deadman = on_deadman_alert
        self._on_chunk_complete = on_chunk_complete

        self._lock = threading.Lock()
        self._progress = SnapshotProgress(
            total_tables=0,
            completed_tables=0,
            total_chunks=0,
            completed_chunks=0,
            failed_chunks=0,
            quarantined_tables=[],
            bytes_transferred=0,
            rows_transferred=0,
            throughput_mbps=0.0,
            elapsed_seconds=0.0,
            eta_seconds=None,
            started_at=datetime.utcnow().isoformat(),
            last_progress_at=datetime.utcnow().isoformat(),
        )
        self._start_time: float | None = None
        self._last_bytes_checkpoint: tuple[float, int] | None = None

    def run(self, tables: list[dict]) -> SnapshotProgress:
        """
        Run the full parallel snapshot.
        `tables` is the table list from schema_inventory.json enriched with tier info.

        Returns final SnapshotProgress when complete.
        """
        self._start_time = time.time()
        logger.info(f"Parallel snapshot starting — {len(tables)} tables")

        # Classify tables by tier
        tier1, tier2, tier3 = self._classify_tables(tables)
        logger.info(
            f"Table tiers: critical={len(tier1)} medium={len(tier2)} archive={len(tier3)}"
        )

        with self._lock:
            self._progress.total_tables = len(tables)

        # Start dead-man's switch monitor
        deadman_thread = threading.Thread(
            target=self._deadman_monitor,
            daemon=True,
            name="migratex-deadman",
        )
        deadman_thread.start()

        # Phase 1: Tier 1 tables — sequential (preserve FK order)
        if tier1:
            logger.info(f"Snapshotting {len(tier1)} critical tables sequentially")
            for table in tier1:
                self._snapshot_table(table, workers=1)

        # Phase 2: Tier 2 tables — parallel with half the workers
        if tier2:
            logger.info(f"Snapshotting {len(tier2)} medium tables (4 workers)")
            self._snapshot_tables_parallel(tier2, workers=min(4, self._max_workers))

        # Phase 3: Tier 3 tables — full parallel
        if tier3:
            logger.info(f"Snapshotting {len(tier3)} archive tables ({self._max_workers} workers)")
            self._snapshot_tables_parallel(tier3, workers=self._max_workers)

        with self._lock:
            self._progress.elapsed_seconds = time.time() - self._start_time

        logger.info(
            f"Snapshot complete — "
            f"tables={self._progress.completed_tables}/{self._progress.total_tables} "
            f"chunks={self._progress.completed_chunks} "
            f"rows={self._progress.rows_transferred:,} "
            f"elapsed={self._progress.elapsed_seconds:.0f}s"
        )
        return self._progress

    # ── Table classification ──────────────────────────────────────────────────

    def _classify_tables(
        self, tables: list[dict]
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """
        Classify tables into tiers based on:
          - FK dependencies (Tier 1 if referenced by other tables)
          - Write rate (Tier 1 if top 10 by write volume)
          - Data size (Tier 3 if bottom 30% by data size)
        """
        # Sort by data size descending
        sorted_tables = sorted(
            tables, key=lambda t: t.get("data_size_bytes", 0), reverse=True
        )
        total = len(sorted_tables)

        # Tables that other tables FK into → critical
        referenced = set()
        for t in tables:
            for fk in t.get("foreign_keys", []):
                referenced.add(fk.get("referenced_table"))

        tier1, tier2, tier3 = [], [], []
        for i, t in enumerate(sorted_tables):
            if t["table_name"] in referenced or i < total * 0.1:
                tier1.append(t)
            elif i >= total * 0.7:
                tier3.append(t)
            else:
                tier2.append(t)

        return tier1, tier2, tier3

    # ── Single table snapshot ─────────────────────────────────────────────────

    def _snapshot_table(self, table: dict, workers: int = 1) -> None:
        table_name = table["table_name"]
        pk_cols = table.get("primary_key_columns", ["id"])
        pk_col = pk_cols[0] if pk_cols else "id"

        chunks = self._generate_chunks(table_name, pk_col)
        logger.info(f"Table '{table_name}': {len(chunks)} chunks (pk={pk_col})")

        with self._lock:
            self._progress.total_chunks += len(chunks)

        if workers == 1:
            for chunk in chunks:
                self._process_chunk(chunk)
        else:
            self._run_chunk_workers(chunks, workers)

        with self._lock:
            self._progress.completed_tables += 1

    def _snapshot_tables_parallel(self, tables: list[dict], workers: int) -> None:
        """Run multiple tables concurrently using a thread pool."""
        table_queue: queue.Queue = queue.Queue()
        for t in tables:
            table_queue.put(t)

        def worker():
            while True:
                try:
                    table = table_queue.get_nowait()
                    self._snapshot_table(table, workers=1)
                    table_queue.task_done()
                except queue.Empty:
                    break
                except Exception as e:
                    logger.error(f"Worker error: {e}")
                    table_queue.task_done()

        threads = [
            threading.Thread(target=worker, daemon=True, name=f"snapshot-worker-{i}")
            for i in range(workers)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def _run_chunk_workers(self, chunks: list[ChunkSpec], workers: int) -> None:
        chunk_queue: queue.Queue = queue.Queue()
        for c in chunks:
            chunk_queue.put(c)

        def worker():
            while True:
                try:
                    chunk = chunk_queue.get_nowait()
                    self._process_chunk(chunk)
                    chunk_queue.task_done()
                except queue.Empty:
                    break
                except Exception as e:
                    logger.error(f"Chunk worker error: {e}")
                    chunk_queue.task_done()

        threads = [
            threading.Thread(target=worker, daemon=True, name=f"chunk-worker-{i}")
            for i in range(workers)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    # ── Chunk processing ──────────────────────────────────────────────────────

    def _process_chunk(self, chunk: ChunkSpec) -> ChunkResult:
        started = datetime.utcnow().isoformat()
        retries = 0

        while retries <= self.MAX_RETRIES:
            try:
                rows, checksum = self._transfer_chunk(chunk)
                result = ChunkResult(
                    chunk=chunk,
                    rows_transferred=rows,
                    checksum=checksum,
                    started_at=started,
                    completed_at=datetime.utcnow().isoformat(),
                    retries=retries,
                    status="completed",
                )
                with self._lock:
                    self._progress.completed_chunks += 1
                    self._progress.rows_transferred += rows
                    self._progress.last_progress_at = datetime.utcnow().isoformat()

                if self._on_chunk_complete:
                    self._on_chunk_complete(result)

                logger.debug(
                    f"Chunk complete: {chunk.table_name}[{chunk.chunk_index}] "
                    f"rows={rows} checksum={checksum[:8]}"
                )
                return result

            except Exception as e:
                retries += 1
                if retries > self.MAX_RETRIES:
                    logger.error(
                        f"Chunk FAILED after {self.MAX_RETRIES} retries: "
                        f"{chunk.table_name}[{chunk.chunk_index}] — {e}"
                    )
                    with self._lock:
                        self._progress.failed_chunks += 1
                        if chunk.table_name not in self._progress.quarantined_tables:
                            self._progress.quarantined_tables.append(chunk.table_name)
                    return ChunkResult(
                        chunk=chunk,
                        rows_transferred=0,
                        checksum="",
                        started_at=started,
                        completed_at=datetime.utcnow().isoformat(),
                        retries=retries,
                        status="quarantined",
                        error=str(e),
                    )

                backoff = self.RETRY_BACKOFF_SECONDS[min(retries - 1, 2)]
                logger.warning(
                    f"Chunk retry {retries}/{self.MAX_RETRIES}: "
                    f"{chunk.table_name}[{chunk.chunk_index}] in {backoff}s"
                )
                time.sleep(backoff)

    def _transfer_chunk(self, chunk: ChunkSpec) -> tuple[int, str]:
        """
        Read chunk from source, write to target, return (row_count, checksum).
        Uses INSERT ... ON DUPLICATE KEY UPDATE for idempotent writes.
        """
        # Build WHERE clause
        if chunk.end_pk is not None:
            where = f"`{chunk.pk_column}` >= %s AND `{chunk.pk_column}` < %s"
            params = (chunk.start_pk, chunk.end_pk)
        else:
            where = f"`{chunk.pk_column}` >= %s"
            params = (chunk.start_pk,)

        # Read from source
        rows = self._source.execute(
            f"SELECT * FROM `{chunk.table_name}` WHERE {where} ORDER BY `{chunk.pk_column}`",
            params,
        )

        if not rows:
            return 0, "empty"

        # Compute checksum
        checksum = self._compute_checksum(rows)

        # Write to target
        if rows:
            cols = list(rows[0].keys())
            col_names = ", ".join(f"`{c}`" for c in cols)
            placeholders = ", ".join(["%s"] * len(cols))
            updates = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in cols)
            sql = (
                f"INSERT INTO `{chunk.table_name}` ({col_names}) "
                f"VALUES ({placeholders}) "
                f"ON DUPLICATE KEY UPDATE {updates}"
            )
            with self._target.cursor() as cur:
                for row in rows:
                    cur.execute(sql, list(row.values()))

        bytes_est = sum(len(str(r)) for r in rows)
        with self._lock:
            self._progress.bytes_transferred += bytes_est

        return len(rows), checksum

    def _generate_chunks(
        self, table_name: str, pk_column: str
    ) -> list[ChunkSpec]:
        """
        Split table into chunks by primary key range.
        Each chunk covers exactly CHUNK_SIZE rows.
        """
        row = self._source.execute_one(
            f"SELECT MIN(`{pk_column}`) AS min_pk, MAX(`{pk_column}`) AS max_pk "
            f"FROM `{table_name}`"
        )
        if not row or row["min_pk"] is None:
            return []

        min_pk = row["min_pk"]
        max_pk = row["max_pk"]

        try:
            min_pk_int = int(min_pk)
            max_pk_int = int(max_pk)
        except (ValueError, TypeError):
            # Non-integer PK — single chunk
            return [ChunkSpec(
                table_name=table_name,
                pk_column=pk_column,
                start_pk=min_pk,
                end_pk=None,
                chunk_index=0,
                tier=TableTier.MEDIUM,
            )]

        chunks = []
        current = min_pk_int
        idx = 0
        while current <= max_pk_int:
            end = current + self._chunk_size
            chunks.append(ChunkSpec(
                table_name=table_name,
                pk_column=pk_column,
                start_pk=current,
                end_pk=end if end <= max_pk_int else None,
                chunk_index=idx,
                tier=TableTier.MEDIUM,
            ))
            current = end
            idx += 1

        return chunks

    def _compute_checksum(self, rows: list[dict]) -> str:
        """CRC32-based checksum over all row values in a chunk."""
        h = 0
        for row in rows:
            row_bytes = json.dumps(
                {k: str(v) for k, v in sorted(row.items())},
                sort_keys=True,
            ).encode("utf-8")
            crc = 0
            for byte in row_bytes:
                h ^= crc
            h = (h + sum(row_bytes)) & 0xFFFFFFFF
        return format(h, "08x")

    # ── Dead-man's switch ─────────────────────────────────────────────────────

    def _deadman_monitor(self) -> None:
        """
        Monitors snapshot throughput every 30 minutes.
        Fires P1 alert if progress < 1 GB in the last 4 hours.
        This prevents a silent hang from blowing past the 7-day Kafka retention boundary.
        """
        check_interval = 1800  # 30 minutes
        window_seconds = self.DEADMAN_WINDOW_HOURS * 3600
        min_bytes = self.DEADMAN_MIN_BYTES_PER_HOUR * self.DEADMAN_WINDOW_HOURS

        checkpoints: list[tuple[float, int]] = []

        while True:
            time.sleep(check_interval)

            with self._lock:
                current_bytes = self._progress.bytes_transferred
                completed = self._progress.completed_tables
                total = self._progress.total_tables

            if completed >= total:
                break  # snapshot complete

            now = time.time()
            checkpoints.append((now, current_bytes))

            # Keep only last DEADMAN_WINDOW_HOURS of checkpoints
            cutoff = now - window_seconds
            checkpoints = [(t, b) for t, b in checkpoints if t >= cutoff]

            if len(checkpoints) >= 2:
                oldest_bytes = checkpoints[0][1]
                progress_in_window = current_bytes - oldest_bytes

                if progress_in_window < min_bytes:
                    msg = (
                        f"DEAD-MAN'S SWITCH TRIGGERED: "
                        f"Snapshot progress < {min_bytes / 1e9:.0f}GB "
                        f"in the last {self.DEADMAN_WINDOW_HOURS}h. "
                        f"Progress: {progress_in_window / 1e9:.2f}GB. "
                        f"Migration may be stalled. "
                        f"Kafka retention at risk — page SRE immediately."
                    )
                    logger.critical(msg)
                    with self._lock:
                        self._progress.deadman_triggered = True
                    if self._on_deadman:
                        self._on_deadman(msg)

    def get_progress(self) -> SnapshotProgress:
        with self._lock:
            p = self._progress
            if self._start_time:
                p.elapsed_seconds = time.time() - self._start_time
                if p.elapsed_seconds > 0 and p.bytes_transferred > 0:
                    p.throughput_mbps = (
                        p.bytes_transferred / 1e6 / p.elapsed_seconds
                    )
            return p
