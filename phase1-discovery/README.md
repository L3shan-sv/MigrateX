# MigrateX — Phase 1: Discovery & Risk Profiling

> **Status:** Pre-migration gate. No data moves until all exit criteria are signed off.
> **Timeline:** Weeks 1–2
> **Entry gate:** Project kickoff approved
> **Exit gate:** Cost & Risk Estimate approved by operator. Isolation Forest calibrated.

---

## What this phase does

Phase 1 establishes the complete factual foundation for the migration. It answers every question the PONR engine and anomaly detector need before a single byte of production data moves:

- How big is the database? What is growing fastest?
- What does healthy database behaviour look like?
- Where is the PII? Which tier does it fall into?
- At what point does rolling back become more expensive than proceeding?
- Can the target infrastructure handle the production load?

---

## Quick start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp config/.env.example config/.env
# Edit config/.env with your source and target DB credentials

# 3. Run the full pipeline
python main.py run \
  --ponr-data-gb 100 \
  --ponr-write-mbps 50 \
  --ponr-network-gbps 1.0 \
  --ponr-sla-usd 5000

# 4. Run tests (no DB required — uses synthetic data)
pytest tests/ -v
```

---

## Individual commands

```bash
# Schema inventory only
python main.py scan

# Traffic profile only
python main.py profile

# PONR pre-migration estimate only
python main.py ponr-estimate \
  --data-gb 100 \
  --write-rate-mbps 50 \
  --network-mean-gbps 1.0 \
  --sla-usd 5000

# Train anomaly detector (synthetic data for testing)
python main.py train-baseline --synthetic --samples 2016
```

---

## Artifacts produced

| File | Description |
|------|-------------|
| `artifacts/schema_inventory.json` | Full table/column/index manifest |
| `artifacts/fk_dependency_graph.json` | Foreign key dependency edges |
| `artifacts/pii_surface_map.json` | PII candidates by tier (1=direct, 2=quasi, 3=contextual) |
| `artifacts/traffic_profile.json` | QPS, P99 latency, write amplification, daily patterns |
| `artifacts/ponr_phase1_estimate.json` | Monte Carlo rollback cost distribution by progress checkpoint |
| `artifacts/model/isolation_forest.pkl` | Trained Isolation Forest model |
| `artifacts/model/model_metadata.json` | Training metadata and calibration results |

---

## Components

### `src/scanner/schema.py` — Schema inventory scanner
Reads `information_schema` to produce a full structural inventory of the source database. Identifies PII candidates using name heuristics and data type analysis. Builds the FK dependency graph used by the PII redaction engine in Phase 4.

### `src/profiler/traffic.py` — Traffic profiler
Reads `performance_schema` to profile write rates, latency distributions, and connection pool utilisation. Outputs the write-rate input for the PONR Monte Carlo simulation.

### `src/ponr/engine.py` — Monte Carlo PONR engine
The core of MigrateX. Runs 10,000 simulations per evaluation to produce a P5/P50/P95 rollback cost distribution. In Phase 1, runs a pre-migration estimate. In Phase 5, runs continuously to monitor the Event Horizon.

### `src/ml_baseline/anomaly.py` — Isolation Forest anomaly detector
Trains on 14-day baseline heartbeat signals. In Phase 1, we train on synthetic data for testing. In production, replace with real signals collected from `performance_schema` over 14 days before starting Phase 2.

---

## Phase 1 exit criteria

All items must be completed and signed off before Phase 2 begins:

- [ ] Schema inventory complete — zero unknown tables
- [ ] PII surface map reviewed — all Tier 3 fields operator-classified
- [ ] FK dependency graph generated and verified
- [ ] Traffic profile completed over minimum 7-day window
- [ ] Isolation Forest trained and calibration check passed
- [ ] Alert threshold reviewed and operator-approved
- [ ] PONR pre-migration estimate reviewed — P95 within acceptable bounds
- [ ] Target instance specification confirmed against capacity model
- [ ] Network path established and 24h bandwidth test recorded

---

## Running in production

For production use, replace the synthetic baseline in `train-baseline` with real 14-day signal collection from `performance_schema`. Minimum 2,016 samples required (14 days × 144 samples/day at 10-second resolution).

The source database user requires `SELECT` on `information_schema`, `performance_schema`, and the source database. No write permissions are needed or used.

```sql
CREATE USER 'migratex_reader'@'%' IDENTIFIED BY 'your_password';
GRANT SELECT ON information_schema.* TO 'migratex_reader'@'%';
GRANT SELECT ON performance_schema.* TO 'migratex_reader'@'%';
GRANT SELECT ON your_database.* TO 'migratex_reader'@'%';
FLUSH PRIVILEGES;
```
