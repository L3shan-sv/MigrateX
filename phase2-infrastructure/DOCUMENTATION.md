# Phase 2 Technical Documentation

## Architecture

```
Source MySQL (edge — binlog ROW format)
        │
        │ binlog stream
        ▼
Debezium MySQL Connector (Kafka Connect)
        │
        │ JSON change events
        ▼
Kafka Cluster (3 brokers, RF=3, 7-day retention)
  Topics: migratex.airbnb_prod.{table}  × N tables
  System: migratex.schema-history
  System: migratex.dlq
        │
        │ consumer group: migratex-sink
        ▼
MigrateX Sink Consumer (8 worker threads)
        │
        ├── GTID deduplication check ──→ _migratex_gtid_dedup table
        │   (same transaction as data write — atomic)
        │
        └── Data write ──→ Target MySQL (cloud)
                               │
                               ├── _migratex_fencing (epoch audit)
                               └── Application tables
```

---

## GTID Deduplication — exact mechanism

### Why GTID and not offset?

Kafka guarantees **at-least-once delivery**. A consumer can receive the same message multiple times after a restart. Kafka offsets are not globally unique across partition reassignments. MySQL GTIDs are globally unique per committed transaction — the same transaction can never produce two different GTIDs.

### Transaction protocol

```python
conn.start_transaction()

try:
    # Step 1: GTID insert — atomic uniqueness gate
    cursor.execute(
        "INSERT INTO _migratex_gtid_dedup (gtid, table_name, operation) VALUES (%s, %s, %s)",
        (event.gtid, event.table, event.operation)
    )
    # If IntegrityError (errno 1062) → ROLLBACK → return 'deduplicated'

    # Step 2: Apply the change (only if Step 1 succeeded)
    cursor.execute("INSERT INTO bookings ... ON DUPLICATE KEY UPDATE ...")

    conn.commit()
    return 'applied'

except IntegrityError as e:
    if e.errno == 1062:  # Duplicate entry
        conn.rollback()
        return 'deduplicated'
    raise
```

The GTID insert and the data write are in the **same transaction**. Either both commit or neither commits. There is no window where a GTID can be recorded without its data write, or a data write can succeed without its GTID record.

### Deduplication table purge

```sql
-- Run daily via Phase 6 scheduled maintenance
DELETE FROM _migratex_gtid_dedup
WHERE applied_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
```

---

## etcd Lease Fencing — exact protocol

### Epoch increment (atomic CAS)

```python
# Atomically read and increment epoch via etcd transaction
current_value, _ = client.get(EPOCH_KEY)
current = int(current_value) if current_value else 0
new_epoch = current + 1

success = client.transaction(
    compare=[client.transactions.value(EPOCH_KEY) == current_value],
    success=[client.transactions.put(EPOCH_KEY, str(new_epoch))],
    failure=[],
)
# Retry if CAS fails (another node incremented concurrently)
```

### Fencing violation detection

Every write to the target database records its epoch in `_migratex_fencing`. The `FencingTokenRecorder` maintains an in-memory `max_seen_epoch`. Any write with `epoch < max_seen_epoch` is rejected before it touches data.

```
Timeline:
  T=0: Edge acquires epoch 1. Writes epoch 1.
  T=X: Cloud acquires epoch 2.
  T=X+1: Stale edge write arrives with epoch 1.
           max_seen_epoch = 2.
           1 < 2 → REJECTED → logged to audit trail.
```

### Lease heartbeat

The etcd lease has TTL=10 seconds. The heartbeat thread fires every 3 seconds:

```
TTL: 10s
Heartbeat: 3s
Tolerance: 3 consecutive failures × 3s = 9s before lease expires
Buffer: 1s between last heartbeat and TTL expiry
```

If 3 consecutive heartbeats fail, the `on_lease_lost` callback fires. In MigrateX, this callback triggers an autonomous pause and pages the SRE.

---

## Parallel Snapshot — chunking algorithm

### Table tier classification

```
Referenced by FK edges  →  Tier 1 (critical, sequential)
Top 10% by data size    →  Tier 1
Middle 60%              →  Tier 2 (parallel, 4 workers)
Bottom 30%              →  Tier 3 (parallel, 8 workers)
```

### PK range chunking

```python
# Given table with PKs 1..1,500,000 and chunk_size=500,000:
chunks = [
    ChunkSpec(start_pk=1,       end_pk=500_001),   # chunk 0
    ChunkSpec(start_pk=500_001, end_pk=1_000_001),  # chunk 1
    ChunkSpec(start_pk=1_000_001, end_pk=None),     # chunk 2 — open-ended
]
```

The last chunk is always open-ended (`end_pk=None`) to capture any rows inserted during the snapshot window.

### Dead-man's switch math

```
Minimum acceptable throughput: 1 GB/hour
Monitoring window: 4 hours
Minimum bytes in window: 4 GB

If bytes_transferred_in_window < 4 GB → P1 ALERT

Why 4 hours?
  7-day retention − 4-hour warning = 163 hours of buffer
  At 1 Gbps, 100 TB takes 222 hours
  4-hour warning gives time to diagnose and recover
  before the retention boundary (7 days from oldest unprocessed event) is hit
```

---

## Kafka topic design decisions

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| One topic per table | ✅ | Independent consumer parallelism, per-table retention, clean DLQ isolation |
| 12 partitions | | Matches target DB write parallelism derived from Phase 1 traffic profile |
| Replication factor 3 | | Survives 1 broker failure without message loss |
| 7-day retention | | Matches MySQL binlog retention — hard rule from fallback plan |
| LZ4 compression | | Best throughput/CPU trade-off for JSON change events |
| Schema history: infinite | | Schema history must never be purged — DDL replay depends on it |
| DLQ retention: 28 days | | 4× normal retention for failed event investigation |

---

## Metrics reference

| Metric | Type | Alert threshold |
|--------|------|----------------|
| `migratex_cdc_lag_seconds` | Gauge | > 120s → HIGH |
| `migratex_sink_events_deduplicated_total` | Counter | Monitor only |
| `migratex_sink_events_failed_total` | Counter | > 0 → MEDIUM |
| `migratex_sink_events_dlq_total` | Counter | > 10 → HIGH |
| `migratex_snapshot_deadman_triggered` | Gauge | = 1 → CRITICAL |
| `migratex_snapshot_throughput_mbps` | Gauge | < 10 → MEDIUM |
| `migratex_lease_healthy` | Gauge | = 0 → CRITICAL |
| `migratex_fencing_violations_total` | Counter | > 0 → CRITICAL |
| `migratex_replication_lag_seconds` | Gauge | Fed to PONR engine |

---

## Test coverage

```
tests/test_phase2.py
├── TestSinkConsumer (10 tests)
│   ├── Apply INSERT returns 'applied'
│   ├── Applying same GTID twice returns 'deduplicated'
│   ├── Metrics increment on apply
│   ├── Dedup count increments correctly
│   ├── Parse Debezium payload correctly
│   ├── Tombstone (null value) returns None
│   ├── Invalid JSON returns None
│   ├── UPDATE uses before image
│   ├── DELETE applies correctly
│   └── Last GTID updated after apply
│
├── TestFencingTokenRecorder (5 tests)
│   ├── Valid epoch accepted
│   ├── Stale epoch rejected
│   ├── Equal epoch accepted
│   ├── Max epoch increases monotonically
│   └── Concurrent epoch validation is thread-safe
│
├── TestAuthorityEnum (3 tests)
│
├── TestParallelSnapshotCoordinator (8 tests)
│   ├── Chunk count correct for given PK range
│   ├── Last chunk is open-ended
│   ├── Empty table returns no chunks
│   ├── Chunk spec has correct fields
│   ├── Table classification separates tiers
│   ├── Checksum is deterministic
│   ├── Checksum differs for different rows
│   └── Progress starts at zero
│
├── TestKafkaTopicManager (5 tests)
│   ├── Topic name format correct
│   ├── DLQ topic name correct
│   ├── Schema history topic name correct
│   ├── 7-day retention constant correct
│   └── Compression type is LZ4
│
└── TestIdempotencyValidator (1 test)
    └── Double write correctly prevented
```

Run: `pytest tests/ -v --tb=short`
