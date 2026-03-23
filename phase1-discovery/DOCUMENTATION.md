# Phase 1 Technical Documentation

## Architecture

Phase 1 is a read-only discovery pipeline. It connects to the source database via a read replica and produces machine-readable artifacts. Nothing is written to the source. Nothing is written to the target. The only writes are to the local `artifacts/` directory.

```
Source DB (read replica)
        │
        ├── information_schema ──→ SchemaScanner ──→ schema_inventory.json
        │                                         ──→ pii_surface_map.json
        │                                         ──→ fk_dependency_graph.json
        │
        ├── performance_schema ──→ TrafficProfiler ──→ traffic_profile.json
        │
        └── (synthetic / real)  → AnomalyDetector ──→ isolation_forest.pkl
                                                    ──→ model_metadata.json
                                                    
Traffic profile + network profile
        │
        └──→ PONREngine ──→ ponr_phase1_estimate.json
```

---

## PONR Engine — Monte Carlo cost model

### Cost components

The rollback cost model has four components, each independently sampled per simulation:

**1. Egress cost**
```
egress_cost = (bytes_transferred / 1e9) × $0.09/GB
```
This is deterministic — it grows linearly with bytes transferred. The variance comes from how much has been transferred at the moment of evaluation.

**2. Re-sync cost**
```
diverged_bytes = replication_lag_seconds × write_rate_bytes_per_sec
resync_seconds = (diverged_bytes / 1e9) / rollback_throughput_gbps × 3600
resync_cost = (resync_seconds / 3600) × $5,000/hour
```
Rollback throughput is sampled from `Normal(0.5, σ/2)` — half the forward bandwidth mean, with proportional variance.

**3. Downtime cost**
```
downtime_seconds = 60 (fixed: lease release + DNS swing) + resync_seconds
downtime_cost = (downtime_seconds / 3600) × $5,000/hour
```

**4. Merkle repair cost**
```
dirty_bytes = seconds_since_last_clean_merkle × write_rate × 0.001
dirty_resync_seconds = (dirty_bytes / 1e9) / rollback_throughput × 3600
merkle_repair_cost = (dirty_resync_seconds / 3600) × $5,000/hour
```
The 0.001 factor assumes 0.1% of writes since the last Merkle check require repair. Conservative — real divergence rate will be lower.

### Event Horizon estimation

```
cost_growth_rate = (throughput_gbps × $0.09/GB) per second
seconds_to_horizon = (sla_threshold - current_p95) / cost_growth_rate
```

Linear extrapolation — conservative. Real P95 grows faster as high-write-rate tables transfer last (they are deferred in the parallel snapshot strategy).

### Why not a PID controller?

PID controllers are designed for continuous physical systems with stable setpoints. Error budget burn rate is discrete and event-driven. A PID tuned for "normal" burn rate will oscillate when the rate is bursty (which it always is during migrations). We use threshold-with-hysteresis instead: two time windows (1h catastrophic, 6h slow regression), both must agree before an alert fires. No tuning required.

---

## Isolation Forest — anomaly detection

### Why Isolation Forest?

Isolation Forest detects anomalies by how quickly it can isolate a data point from the rest. Anomalous points are isolated in fewer splits. It works well on:
- Unlabelled data (we have no "anomaly" labels from production)
- High-dimensional correlated features (all 10 signals are correlated)
- Mixed-scale features (latency in ms, WAL in bytes/s, counts)

Alternative: One-Class SVM. Rejected because it is sensitive to hyperparameter tuning and does not scale well to 14 days × 144 samples/day = 2,016+ training points at inference time.

### Feature normalisation

All features are normalised via `StandardScaler` before training and inference. This is essential because the features have wildly different scales (1ms vs 50,000,000 bytes/s). The scaler parameters (mean, std) are stored in `model_metadata.json` and applied at inference time.

### Score normalisation

Isolation Forest's `decision_function` returns raw scores where:
- Positive = more normal (further from anomaly boundary)
- Negative = more anomalous

We flip and min-max normalise to 0→1 where 1 = most anomalous. This makes the alert threshold human-interpretable (`alert_threshold=0.7` means "score in the top 30% of anomalousness relative to the training distribution").

### Calibration

After training, three known failure modes are injected into a staging replica and scored:
1. Simulated 30-second replication lag spike → expected score > 0.6
2. Simulated connection pool exhaustion (80% held by idle queries) → expected score > 0.65
3. Simulated 200ms INSERT delay → expected score > 0.7

The production alert threshold is set at the lowest score that correctly identifies all three failure modes with zero false positives during the 14-day baseline window.

---

## PII Classification

### Tier 1 — Direct PII
Columns whose names match known PII patterns (`email`, `phone`, `first_name`, etc.). These are HMAC-SHA256 hashed before any data leaves the local environment. The hash key rotates per migration run. Consistent within a run (same plaintext = same hash), preventing FK integrity violations.

### Tier 2 — Quasi-PII
Columns like `zipcode`, `latitude`, `birth_year`. These are hashed with the same mechanism. Cross-reference analysis is required — a quasi-PII field alone is not identifying, but combined with other fields it may be.

### Tier 3 — Contextual PII
Long text fields (TEXT, MEDIUMTEXT, JSON over 100 chars) that *may* contain PII embedded in free text (reviews, messages, notes). These are flagged for operator review. They are not automatically hashed because hashing free text destroys its utility. The operator must decide: redact, truncate, or pass through with explicit sign-off.

---

## Database permissions required

```sql
-- Minimum permissions for Phase 1 (read-only)
GRANT SELECT ON information_schema.* TO 'migratex_reader'@'%';
GRANT SELECT ON performance_schema.* TO 'migratex_reader'@'%';
GRANT SELECT ON your_database.* TO 'migratex_reader'@'%';

-- Performance schema must have consumers enabled
UPDATE performance_schema.setup_consumers
SET ENABLED = 'YES'
WHERE NAME IN (
    'events_statements_summary_by_digest',
    'table_io_waits_summary_by_table',
    'global_variables'
);
```

---

## Configuration reference

| Setting | Default | Description |
|---------|---------|-------------|
| `PONR_SIMULATIONS` | 10,000 | Monte Carlo draw count. Higher = more accurate P95. 10k takes ~2s on modern hardware. |
| `PONR_SLA_THRESHOLD_USD` | 5,000 | Rollback cost that triggers BLOCK recommendation. Set based on your downtime cost model. |
| `PONR_BLOCK_CONFIDENCE` | 0.95 | P-value used for the BLOCK gate. 0.95 = P95 must be below threshold. |
| `ANOMALY_CONTAMINATION` | 0.05 | Fraction of training data assumed to be anomalous. Default 5% is appropriate for production. |
| `ANOMALY_ALERT_THRESHOLD` | 0.7 | Normalised score above which an autonomous pause fires. Calibrated during Phase 1. |
| `PII_ENTROPY_THRESHOLD` | 3.5 | Shannon entropy threshold for Tier 3 flagging. Lower = more columns flagged. |
| `SCAN_THREADS` | 8 | Parallel workers for the schema scan. Keep below 70% of replica connection pool. |
| `SNAPSHOT_CHUNK_SIZE` | 500,000 | Rows per parallel snapshot chunk (used in Phase 2 coordinator). |

---

## Test coverage

```
tests/test_phase1.py
├── TestPONREngine (10 tests)
│   ├── Percentile ordering (P5 ≤ P50 ≤ P95 ≤ P99)
│   ├── Rollback feasibility at low SLA
│   ├── Cost increases with migration progress
│   ├── BLOCK recommendation when P95 > SLA
│   ├── PROCEED recommendation when well below SLA
│   ├── Simulation count in output
│   ├── Event horizon positivity
│   ├── Pre-migration estimate checkpoint count
│   ├── Degraded network raises P95
│   └── Zero bytes transferred = minimal cost
│
├── TestAnomalyDetector (6 tests)
│   ├── Trains on synthetic data
│   ├── Normal signal scores below threshold
│   ├── Anomalous signal scores above threshold
│   ├── Model persists and loads correctly
│   ├── Unfit detector raises on score
│   └── Batch scoring matches individual
│
├── TestPIIClassifier (8 tests)
│   ├── Email → Tier 1
│   ├── Phone number → Tier 1
│   ├── Zipcode → Tier 2
│   ├── Long text → Tier 3
│   ├── Non-PII column not classified
│   ├── listing_id not PII
│   ├── first_name → Tier 1
│   └── latitude → Tier 2
│
└── TestSyntheticBaseline (4 tests)
    ├── Correct sample count
    ├── Valid feature vectors
    ├── Buffer pool hit rate in valid range
    └── Non-negative counts
```

Run with: `pytest tests/ -v --tb=short`
