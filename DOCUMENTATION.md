# MigrateX — Root Technical Documentation

## System overview

MigrateX is structured as six independent phases. Each phase produces artifacts consumed by the next. No phase begins until the previous phase's exit criteria are signed off by the designated operator.

```
Phase 1: Discovery
  └─ Artifacts: schema_inventory, traffic_profile, ponr_estimate, isolation_forest model
       │
Phase 2: Infrastructure
  └─ Artifacts: CDC pipeline live, etcd cluster healthy, synthetic validation passed
       │
Phase 3: Chaos Validation
  └─ Artifacts: resilience_score > 95, all 7 experiments passed
       │
Phase 4: Dark Launch
  └─ Artifacts: 72h zero Class A divergences, semantic audit clean
       │
Phase 5: Cutover
  └─ Artifacts: cloud target authoritative, edge read-only, Merkle clean
       │
Phase 6: Hardening
  └─ Artifacts: edge decommissioned, models retrained, SRE report published
```

---

## Data flow

```
Source MySQL (edge)
    │
    ├── binlog (ROW format) ──→ Debezium ──→ Kafka topics
    │                                            │
    │                                       Sink Consumer
    │                                       (GTID dedup)
    │                                            │
    │                                       Target MySQL (cloud)
    │                                            │
    │                                       Merkle Engine
    │                                       (dirty-block scan)
    │
    ├── performance_schema ──→ Traffic Profiler ──→ PONR Engine
    │
    ├── heartbeat signals ──→ Isolation Forest ──→ Anomaly Score
    │
    └── write path ──→ etcd lease ──→ Fencing Token
                        (one authority node at all times)
```

---

## Component interfaces

### PONR Engine API

**Input: `MigrationState`**
```python
MigrationState(
    total_data_bytes: int,
    bytes_transferred: int,
    write_rate_bytes_per_sec: float,
    replication_lag_seconds: float,
    elapsed_seconds: float,
    last_clean_merkle_seconds: float,
)
```

**Input: `NetworkProfile`**
```python
NetworkProfile(
    mean_throughput_gbps: float,
    std_throughput_gbps: float,
    rtt_ms_p50: float,
    rtt_ms_p99: float,
    packet_loss_pct: float,
)
```

**Output: `PONRDistribution`**
```python
PONRDistribution(
    p5_usd: float,
    p50_usd: float,
    p95_usd: float,
    p99_usd: float,
    mean_usd: float,
    std_usd: float,
    rollback_feasible: bool,        # True if P95 < SLA threshold
    event_horizon_seconds: float,   # est. seconds until P95 breaches SLA
    simulations_run: int,
    recommendation: "PROCEED" | "CAUTION" | "BLOCK",
)
```

**Gate logic:**
```
if P95 >= SLA_THRESHOLD → BLOCK (cutover prohibited)
if P95 >= SLA_THRESHOLD × 0.80 → CAUTION (prepare Tier 3 sequence)
else → PROCEED
```

---

### Anomaly Detector API

**Input: `HeartbeatSignal`** (10-second observation window)
```python
HeartbeatSignal(
    p99_select_latency_ms: float,
    p99_write_latency_ms: float,
    replication_lag_seconds: float,
    wal_write_rate_bytes_per_sec: float,
    buffer_pool_hit_rate_pct: float,
    conn_active: int,
    conn_idle: int,
    conn_waiting: int,
    deadlock_count_per_min: float,
    long_query_count: int,
)
```

**Output: `AnomalyScore`**
```python
AnomalyScore(
    score: float,               # 0.0 (normal) → 1.0 (anomaly)
    is_anomaly: bool,           # True if score >= alert_threshold
    threshold: float,
    recommendation: "NORMAL" | "MONITOR_CLOSELY" | "AUTONOMOUS_PAUSE",
)
```

---

## Artifact schema reference

### `schema_inventory.json`
```json
{
  "database": "airbnb_prod",
  "tables": [
    {
      "table_name": "bookings",
      "engine": "InnoDB",
      "row_count_estimate": 150000000,
      "data_size_bytes": 45000000000,
      "primary_key_columns": ["id"],
      "columns": [...],
      "foreign_keys": [...],
      "has_triggers": false
    }
  ],
  "total_data_bytes": 98000000000,
  "fk_dependency_edges": [...]
}
```

### `pii_surface_map.json`
```json
{
  "users": [
    {"column": "email", "type": "varchar", "pii_tier": 1},
    {"column": "phone_number", "type": "varchar", "pii_tier": 1},
    {"column": "zipcode", "type": "varchar", "pii_tier": 2}
  ],
  "listings": [
    {"column": "description", "type": "text", "pii_tier": 3}
  ]
}
```

### `ponr_phase1_estimate.json`
```json
{
  "total_data_gb": 100.0,
  "estimated_duration_hours": {"p50": 22.2, "p95": 55.6},
  "sla_threshold_usd": 5000.0,
  "rollback_cost_by_progress": [
    {
      "progress_pct": 25.0,
      "p5_usd": 12.50,
      "p50_usd": 42.30,
      "p95_usd": 198.50,
      "rollback_feasible": true,
      "recommendation": "PROCEED"
    }
  ]
}
```

---

## Monitoring and alerting

All components publish metrics to Prometheus on port 8000 (MigrateX exporter).

| Metric | Type | Description |
|--------|------|-------------|
| `migratex_ponr_p95_usd` | Gauge | Current P95 rollback cost |
| `migratex_ponr_recommendation` | Gauge | 0=PROCEED, 1=CAUTION, 2=BLOCK |
| `migratex_anomaly_score` | Gauge | Latest Isolation Forest score |
| `migratex_replication_lag_seconds` | Gauge | CDC consumer lag |
| `migratex_merkle_divergence_pct` | Gauge | Current Merkle divergence |
| `migratex_bytes_transferred_total` | Counter | Total bytes transferred |
| `migratex_migration_progress_pct` | Gauge | Migration progress 0–100 |
| `migratex_gtid_dedup_total` | Counter | GTID deduplication events |

### Alert thresholds

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| `PONRBlock` | `ponr_p95_usd >= sla_threshold` | Critical | Block cutover immediately |
| `PONRCaution` | `ponr_p95_usd >= sla_threshold * 0.8` | High | Prepare Tier 3 sequence |
| `AnomalyDetected` | `anomaly_score >= 0.7` | High | Autonomous pause |
| `ReplicationLagHigh` | `replication_lag_seconds > 120` | High | Scale sink workers |
| `MerkleDivergence` | `merkle_divergence_pct > 0.01` | Critical | Block + repair |
| `BurnRateCritical` | Burn > 10x (1h window) | Critical | Emergency throttle |
| `BurnRateHigh` | Burn > 5x (6h window) | Medium | Operator notification |

---

## Environment variables (root)

Each phase has its own `.env`. The root Docker Compose passes shared variables.

| Variable | Default | Shared across |
|----------|---------|---------------|
| `SOURCE_HOST` | `source` | All phases |
| `TARGET_HOST` | `target` | Phase 2–6 |
| `KAFKA_BOOTSTRAP` | `kafka:29092` | Phase 2–5 |
| `ETCD_ENDPOINTS` | `etcd0:2379,etcd1:2379,etcd2:2379` | Phase 2–5 |
| `PROMETHEUS_PUSHGATEWAY` | `prometheus:9091` | All phases |
| `PONR_SLA_THRESHOLD_USD` | `5000` | Phase 1, 5 |
| `ANOMALY_ALERT_THRESHOLD` | `0.7` | Phase 1, 2–5 |

---

## Dependency graph between phases

```
Phase 1 produces:
  ponr/engine.py          → used by Phase 5 (live PONR monitor)
  ml_baseline/anomaly.py  → used by Phase 2+ (live anomaly scoring)
  schema_inventory.json   → used by Phase 2 (Kafka topic creation)
  pii_surface_map.json    → used by Phase 4 (redaction engine)
  fk_dependency_graph.json → used by Phase 4 (redaction ordering)

Phase 2 produces:
  sink consumer           → used by Phase 3 (chaos target)
  etcd lease client       → used by Phase 5 (cutover)
  GTID dedup engine       → used by Phase 3–5

Phase 3 produces:
  resilience score        → gates Phase 4 entry

Phase 4 produces:
  semantic audit report   → gates Phase 5 entry

Phase 5 produces:
  cutover completion event → gates Phase 6 entry
```
