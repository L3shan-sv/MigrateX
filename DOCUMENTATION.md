# MigrateX — Root Technical Documentation

## System overview

MigrateX is six independent phases that compose into a single migration system. Each phase produces artifacts consumed by the next. No phase begins until the previous phase's exit criteria are signed off by the designated operator.

```
Phase 1: Discovery
  └─ Artifacts: schema_inventory.json, traffic_profile.json,
                ponr_phase1_estimate.json, isolation_forest.pkl
     │
Phase 2: Infrastructure
  └─ Artifacts: CDC pipeline live, etcd cluster healthy,
                GTID sink validated, synthetic load passed
     │
Phase 3: Chaos Validation
  └─ Artifacts: resilience_report.json (score ≥ 95),
                all 7 experiments PASSED
     │
Phase 4: Dark Launch
  └─ Artifacts: 72h zero Class A divergences,
                pii_redaction_validated, finops_active
     │
Phase 5: Cutover
  └─ Artifacts: cloud target authoritative, edge read-only,
                post_cutover_report.json (24h validation passed)
     │
Phase 6: Hardening
  └─ Artifacts: edge terminated, isolation_forest_cloud.pkl,
                ponr_cloud_params.json, sre-report.json, 4 runbooks
```

---

## Data flow — end to end

```
Source MySQL (edge, ROW binlog, GTID ON)
    │
    ├── information_schema ──→ SchemaScanner ──→ schema_inventory.json
    │                                         ──→ pii_surface_map.json
    │                                         ──→ fk_dependency_graph.json
    │
    ├── performance_schema ──→ TrafficProfiler ──→ traffic_profile.json
    │                      ──→ AnomalyDetector.fit() ──→ isolation_forest.pkl
    │
    ├── binlog (Debezium) ──→ Kafka (per-table topics, RF=3, 7d TTL)
    │                              │
    │                         SinkConsumer (8 workers)
    │                         ├── GTID dedup (atomic tx)
    │                         └── Target MySQL (cloud)
    │
    ├── heartbeat signals ──→ AnomalyDetector.score() ──→ anomaly_score
    │
    └── write commits ──→ ShadowProxy.compare_write()
                          ├── Class A → autonomous pause + 72h clock restart
                          ├── Class B → operator review
                          └── Class C → capacity review

PONR Engine (continuous, Phase 1 estimate → Phase 5 live monitor)
    ├── Input: bytes_transferred, lag, write_rate, network profile
    ├── Process: 10,000 Monte Carlo simulations
    └── Output: P5/P50/P95 distribution → PROCEED | CAUTION | BLOCK

etcd Lease (write authority arbitration)
    ├── Edge holds epoch N during migration
    ├── Cloud acquires epoch N+1 at cutover
    └── All epoch N writes rejected after N+1 acquired
```

---

## Component interfaces — cross-phase dependencies

### Phase 1 → Phase 2
```
schema_inventory.json     → KafkaTopicManager (one topic per table)
pii_surface_map.json      → HMACRedactionEngine (tier classification)
fk_dependency_graph.json  → HMACRedactionEngine (FK processing order)
traffic_profile.json      → PONREngine (write_rate input)
isolation_forest.pkl      → AnomalyDetector.score() (Phase 2+ scoring)
```

### Phase 2 → Phase 3
```
CDC pipeline (live)        → TrafficReplayEngine (replay target)
etcd cluster (live)        → EtcdLeaderKillExperiment
Kafka cluster (live)       → KafkaBrokerFailureExperiment
sink consumer (live)       → ConsumerProcessKillExperiment
GTID dedup engine (live)   → IdempotencyValidator
```

### Phase 3 → Phase 4
```
resilience_report.json     → SREReportGenerator (Phase 6 comparison)
Phase 3 resilience score   → Phase 4 entry gate (must be ≥ 95)
```

### Phase 4 → Phase 5
```
pii_surface_map.json       → HMACRedactionEngine
fk_dependency_graph.json   → HMACRedactionEngine
Shadow audit window state  → Phase 5 entry gate (72h zero Class A)
FinOps arbitrator (live)   → continues through Phase 5
```

### Phase 5 → Phase 6
```
cutover_result.json        → SREReportGenerator
post_cutover_report.json   → Decommission entry gate
ponr_p95_at_cutover        → ModelRetrainingEngine (PONR recalibration input)
```

### Phase 1 → Phase 6 (long-range)
```
ponr_phase1_estimate.json  → SREReportGenerator (deviation baseline)
isolation_forest.pkl       → ModelRetrainingEngine (threshold comparison)
```

---

## PONR engine — complete specification

### Inputs
```python
MigrationState(
    total_data_bytes: int,              # from Phase 1 schema inventory
    bytes_transferred: int,             # live from CDC progress tracker
    write_rate_bytes_per_sec: float,    # live from performance_schema
    replication_lag_seconds: float,     # live from Kafka consumer lag
    elapsed_seconds: float,             # wall clock since Phase 2 start
    last_clean_merkle_seconds: float,   # seconds since last Merkle pass
)
NetworkProfile(
    mean_throughput_gbps: float,        # from 24h bandwidth test (Phase 1)
    std_throughput_gbps: float,         # variance — matters for P95 spread
    rtt_ms_p50: float,
    rtt_ms_p99: float,
    packet_loss_pct: float,
)
```

### Cost model (per simulation draw)
```
egress_cost       = bytes_transferred_gb × $0.09/GB
resync_cost       = (lag_s × write_rate_bs / 1e9) / rollback_tp_gbps × $5,000/h
downtime_cost     = (60s_fixed + resync_s) / 3600 × $5,000/h
merkle_repair     = (since_merkle_s × write_rate_bs × 0.001 / 1e9) / rollback_tp_gbps × $5,000/h

rollback_tp_gbps ~ Normal(0.5, std/2)      ← sampled independently per draw
total_cost = egress + resync + downtime + merkle_repair
```

### Decision gate
```
P95 = np.percentile(costs_10000_draws, 95)

if P95 >= SLA_THRESHOLD:          → BLOCK    (cutover prohibited, gate locked)
if P95 >= SLA_THRESHOLD × 0.80:   → CAUTION  (prepare Tier 3 sequence)
else:                              → PROCEED  (cutover safe)
```

### Evaluation cadence
```
Phase 1:      Pre-migration estimate at 6 progress checkpoints
Phase 2–4:    Every 60 seconds (background monitor)
Phase 5 T-2h: Every 30 seconds (Event Horizon mode)
Phase 5 T-0:  Final evaluation before operator token accepted
Phase 6:      Recalibrated for cloud→cloud (DR, version upgrades)
```

---

## Anomaly detection — complete specification

### Training signal (10-second windows, 14 days = 2,016+ samples)
```
p99_select_latency_ms
p99_write_latency_ms
replication_lag_seconds
wal_write_rate_bytes_per_sec
buffer_pool_hit_rate_pct
conn_active
conn_idle
conn_waiting
deadlock_count_per_min
long_query_count
```

### Scoring pipeline
```
raw_signal → StandardScaler.transform() → IsolationForest.decision_function()
           → flip sign → min-max normalise → score ∈ [0.0, 1.0]

score ≥ alert_threshold (default 0.7) → AUTONOMOUS_PAUSE
score ≥ alert_threshold × 0.8        → MONITOR_CLOSELY
else                                   → NORMAL
```

### Model lifecycle
```
Phase 1:  Train on 14-day edge baseline (real or synthetic for CI)
Phase 2+: Score live heartbeat signals (loaded from pkl)
Phase 6:  Retrain on 14-day cloud-native baseline → isolation_forest_cloud.pkl
```

---

## Divergence classification (Phase 4)

```
Operation: DELETE
  edge=None, target=None  → NONE  (both deleted cleanly)
  edge=None, target=row   → CLASS_A  (not deleted on target — BLOCKS)
  edge=row,  target=None  → CLASS_B  (CDC lag? investigate)

Operation: INSERT / UPDATE
  both None               → CLASS_B  (possible rollback)
  edge=row,  target=None  → CLASS_A  (missing from target — BLOCKS)
  edge=None, target=row   → CLASS_B  (unexpected — investigate)
  both exist, diff values → CLASS_A  (value divergence — BLOCKS)
  NULL vs ""              → CLASS_B  (collation normalisation — soft)

Timing check (any operation):
  target_ms > edge_ms × 2 → CLASS_C  (capacity review)
```

Class A restarts the 72-hour observation window. Zero Class A for 72 continuous hours = Phase 4 exit criterion met.

---

## etcd fencing token protocol

```
State before migration:   epoch = 0 (uninitialised)

Phase 2 provisioning:     Edge orchestrator acquires lease → epoch = 1
                          All writes tagged epoch 1

Phase 5 Step 3 (cutover): Cloud orchestrator CAS-increments epoch
                          epoch: 1 → 2 (atomic via etcd transaction)
                          Cloud acquires lease with epoch 2

Any subsequent write:     Validator checks incoming_epoch >= max_seen_epoch
                          Epoch 1 write arrives → 1 < 2 → REJECTED + audit log
                          Epoch 2 write arrives → 2 >= 2 → ACCEPTED

Lease TTL = 10s, heartbeat = 3s
3 consecutive heartbeat failures → lease_lost callback → autonomous pause
```

---

## Kafka topic design

```
Naming:        {prefix}.{database}.{table}
               migratex.airbnb_prod.bookings

System topics: migratex.schema-history  (retention: infinite)
               migratex.dlq             (retention: 28 days)

Per-table:     partitions = 12 (matches target DB write parallelism)
               replication_factor = 3 (survives 1 broker failure)
               retention_ms = 604_800_000 (7 days — hard floor)
               compression = lz4

Hard rule:     Any topic with retention < 7 days blocks phase advancement.
               Dead-man's-switch fires at 4 days of consumer lag.
               Kafka retention floor = 7 days is non-negotiable.
```

---

## 7-step atomic cutover sequence

```
Step 1: Drain write queue       50ms application buffer held
Step 2: Final Merkle            Top 10 tables by write volume
                                ABORT if divergence > 0.0%
Step 3: Cloud acquires lease    etcd CAS epoch N+1
                                ABORT if: lease failure | RTT > 3× | anomaly ≥ 0.7
Step 4: DNS swing               TTL=1s (pre-set at T-24h)
                                ABORT if: anomaly ≥ 0.7 | DNS failure
Step 5: Flush write buffer      10ms — buffered writes go to cloud
                                ABORT if: anomaly ≥ 0.7
        ── POINT OF COMMITMENT ──────────────────────────────────────────
Step 6: SET GLOBAL read_only=1  Edge becomes read-only
                                No abort — Tier 4 required if Step 7 fails
Step 7: Post-Merkle (all)       Full verification, max 5 minutes
                                FAIL → state = ROLLED_BACK → Tier 4
```

Connection pool drain (the 1290 safety net):
```python
except MySQLError as e:
    if e.errno == 1290:  # read_only
        pool.close_all()
        pool.reconnect()  # new DNS → cloud target
        retry()
```

---

## Prometheus metrics reference

| Metric | Type | Alert threshold | Phase |
|--------|------|----------------|-------|
| `migratex_ponr_p95_usd` | Gauge | ≥ sla_threshold → BLOCK | 2–5 |
| `migratex_anomaly_score` | Gauge | ≥ 0.7 → HIGH | 2–5 |
| `migratex_replication_lag_seconds` | Gauge | > 120s → HIGH | 2–5 |
| `migratex_merkle_divergence_pct` | Gauge | > 0.01% → CRITICAL | 2–6 |
| `migratex_fencing_violations_total` | Counter | > 0 → CRITICAL | 2–5 |
| `migratex_sink_events_dlq_total` | Counter | > 10 → HIGH | 2–5 |
| `migratex_snapshot_deadman_triggered` | Gauge | = 1 → CRITICAL | 2 |
| `migratex_lease_healthy` | Gauge | = 0 → CRITICAL | 2–5 |
| `migratex_shadow_error_rate` | Gauge | > 0.01% → HIGH | 4 |

---

## Fallback plan — 4 tiers

| Tier | Trigger | Actor | RTO | Path |
|------|---------|-------|-----|------|
| **Tier 1** | Anomaly score spike, lag spike | Autonomous | < 10 min | System self-heals, SRE notified after |
| **Tier 2** | Tier 1 unresolved in 10 min | Primary SRE | 10–60 min | Migration pauses, playbook executed, resumes |
| **Tier 3** | P95 < SLA, operator decision | IC | < 5 min | 8-step rollback: release lease → read-write edge → GTID verify → app swing |
| **Tier 4** | P95 ≥ SLA (already breached) | IC + Leadership | 10 min – 4h | Merkle full scan → repair from Kafka WAL if diverged |

Hard rules on all tiers:
- Edge source deletion lock active T+0 through T+10 days — no exceptions
- Kafka retention floor 7 days — dead-man's-switch at 4 days
- Post-mortem mandatory for any Tier 3 or 4 incident
- PONR block cannot be overridden by any operator or software path

---

## Environment variables (shared across phases)

| Variable | Default | Used by |
|----------|---------|---------|
| `SOURCE_HOST` | `source` | All phases |
| `TARGET_HOST` | `target` | Phase 2–6 |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Phase 2–5 |
| `ETCD_ENDPOINTS` | `etcd0:2379,...` | Phase 2–5 |
| `PONR_SLA_THRESHOLD_USD` | `5000` | Phase 1, 5 |
| `PONR_SIMULATIONS` | `10000` | Phase 1, 5 |
| `ANOMALY_ALERT_THRESHOLD` | `0.7` | Phase 1–5 |
| `SNAPSHOT_CHUNK_SIZE` | `500000` | Phase 2 |
| `AUDIT_OBSERVATION_WINDOW_HOURS` | `72` | Phase 4 |
| `JWT_ALGORITHM` | `RS256` | Phase 5 |
| `DECOMMISSION_WINDOW_DAYS` | `10` | Phase 6 |
