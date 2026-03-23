# MigrateX — Phase 2: Infrastructure Provisioning

> **Status:** Entry gate — Phase 1 exit criteria must be signed off before this phase begins.
> **Timeline:** Weeks 3–4
> **Entry gate:** Phase 1 exit criteria signed off
> **Exit gate:** Full pipeline validated on synthetic data. CDC lag < 30s sustained 4 hours. etcd quorum healthy.

---

## What this phase builds

Phase 2 builds every infrastructure component required for the migration before a single byte of production data moves. The phase is complete only when the full pipeline demonstrates end-to-end correctness on synthetic data at production volume.

| Component | What it does |
|-----------|-------------|
| **Debezium CDC connector** | Streams every committed write from MySQL binlog to Kafka |
| **Kafka topics** | Per-table durable event log (12 partitions, 7-day retention, LZ4) |
| **GTID sink consumer** | Reads Kafka, applies to target, deduplicates via GTID atomic check |
| **etcd lease fencing** | Ensures exactly one node holds write authority at any moment |
| **Parallel snapshot coordinator** | Chunks 100TB+ initial load across 8 workers with dead-man's switch |
| **Prometheus metrics** | All pipeline health signals exported for Grafana dashboards |

---

## Quick start

```bash
# 1. Start the full stack (from project root)
docker compose up -d source target kafka zookeeper schema-registry kafka-connect etcd0 etcd1 etcd2

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp config/.env.example config/.env
# Edit config/.env

# 4. Run full provisioning pipeline
python main.py provision --inventory ../phase1-discovery/artifacts/schema_inventory.json

# 5. Run tests (no live infra required)
pytest tests/ -v
```

---

## Individual commands

```bash
# Check binlog config on source MySQL
python main.py check-binlog

# Create all Kafka topics from schema inventory
python main.py create-topics --inventory ./artifacts/schema_inventory.json

# Register Debezium connector
python main.py register-connector

# Check etcd cluster health
python main.py check-lease

# Run synthetic validation
python main.py validate-synthetic
```

---

## The zero double-write guarantee

Every write goes through atomic GTID deduplication:

```sql
BEGIN;
  -- Step 1: Insert GTID (raises IntegrityError if duplicate)
  INSERT INTO _migratex_gtid_dedup (gtid, table_name, operation)
  VALUES ('server-uuid:12345', 'bookings', 'I');

  -- Step 2: Apply the actual change (only reached if Step 1 succeeded)
  INSERT INTO bookings (...) VALUES (...) ON DUPLICATE KEY UPDATE ...;
COMMIT;
```

If the GTID already exists → transaction rolls back → event silently discarded. The data write and the GTID record are atomically coupled — they cannot diverge.

---

## etcd fencing token protocol

```
Epoch N:   Edge holds lease → all writes tagged epoch N
Cutover:   Cloud acquires lease → epoch N+1
           Edge's epoch N < current max epoch N+1
           All subsequent edge writes: REJECTED
```

The fencing token recorder validates every write before it touches data. A stale epoch write is logged to the audit trail and rejected at the DB level — not just at the application level.

---

## Phase 2 exit criteria

- [ ] CDC consumer lag < 30s sustained over 4 hours
- [ ] Initial snapshot complete — Merkle verification passing (divergence = 0)
- [ ] All Kafka topics created with 7-day retention verified
- [ ] Debezium connector in RUNNING state with all tasks healthy
- [ ] etcd cluster: all 3 nodes in quorum (verified via `etcdctl endpoint health`)
- [ ] Fencing token protocol validated — stale-write rejection test passed
- [ ] All 7 Prometheus alert rules configured and firing correctly on simulation
- [ ] Synthetic load validation protocol completed with zero failures
- [ ] Idempotency test passed — applying same GTID twice yields exactly one row

---

## Required MySQL user permissions

```sql
-- Source DB — Debezium connector user
CREATE USER 'debezium'@'%' IDENTIFIED BY 'your_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

-- Target DB — Sink consumer user
CREATE USER 'migratex_writer'@'%' IDENTIFIED BY 'your_password';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX, ALTER ON airbnb_prod.* TO 'migratex_writer'@'%';
FLUSH PRIVILEGES;
```
