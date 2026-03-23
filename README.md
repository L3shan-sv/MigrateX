# MigrateX — The Autonomous Data Fabric

> Risk-quantified, zero-downtime database migration from on-premise to cloud at Airbnb scale.

[![Phase 1](https://img.shields.io/badge/Phase%201-Discovery-brightgreen)]()
[![Phase 2](https://img.shields.io/badge/Phase%202-Infrastructure-brightgreen)]()
[![Phase 3](https://img.shields.io/badge/Phase%203-Chaos-brightgreen)]()
[![Phase 4](https://img.shields.io/badge/Phase%204-Dark%20Launch-brightgreen)]()
[![Phase 5](https://img.shields.io/badge/Phase%205-Cutover-brightgreen)]()
[![Phase 6](https://img.shields.io/badge/Phase%206-Hardening-brightgreen)]()

---

## What is MigrateX?

MigrateX is a production-grade database migration orchestrator that answers the question no existing tool answers:

> *At what exact point does rolling back become more dangerous than pressing forward?*

It replaces the midnight gut call with a Monte Carlo probability model. It replaces "I think it's fine" with P5/P50/P95 rollback cost distributions updated every 60 seconds. It replaces "operator notified" with a cryptographically signed JWT that binds the operator's identity to the exact risk profile they reviewed.

**Reference scenario:** Migrating a 100TB+ MySQL cluster from on-premise edge infrastructure to cloud — Airbnb-scale operational data, globally distributed traffic, 99.99% availability SLA.

---

## Core guarantees

| Guarantee | Mechanism |
|-----------|-----------|
| Zero data loss | Merkle tree O(log N) dirty-block detection + targeted WAL repair |
| Zero double writes | GTID-based atomic deduplication — same transaction as data write |
| Sub-100ms downtime | etcd lease transfer + connection pool drain + DNS TTL=1s |
| Quantified rollback risk | Monte Carlo PONR: 10,000 simulations → P95 gate blocks cutover |
| No silent divergence | Semantic audit — Class A/B/C across 6 critical business logic paths |
| Human always in the loop | RS256-signed JWT operator token required at every cutover gate |
| PONR block is absolute | No software path bypasses a P95 ≥ SLA threshold block |
| Blast radius bounded | All chaos experiments scoped to shadow traffic only |

---

## Project structure

```
migratex/
├── README.md                           ← you are here
├── DOCUMENTATION.md                    ← root technical reference
├── docker-compose.yml                  ← full stack in one command
│
├── phase1-discovery/                   ← Schema scan, traffic profile, PONR init, ML baseline
│   ├── src/ponr/engine.py              ← Monte Carlo PONR simulation engine ★
│   ├── src/ml_baseline/anomaly.py      ← Isolation Forest anomaly detector ★
│   └── tests/ (28 tests)
│
├── phase2-infrastructure/              ← CDC pipeline, GTID sink, etcd lease, snapshot
│   ├── src/sink/consumer.py            ← GTID sink consumer — zero double-write ★
│   ├── src/lease/fencing.py            ← etcd distributed lease fencing ★
│   ├── src/snapshot/coordinator.py     ← Parallel 100TB+ snapshot + dead-man's switch ★
│   └── tests/ (32 tests)
│
├── phase3-chaos/                       ← 7-experiment chaos suite, resilience scoring
│   ├── src/chaos/experiments.py        ← All 7 chaos experiments ★
│   ├── src/resilience/scorer.py        ← 100-point resilience score (gate: ≥ 95)
│   └── tests/ (42 tests)
│
├── phase4-dark-launch/                 ← Shadow writes, semantic audit, PII redaction, FinOps
│   ├── src/shadow/proxy.py             ← CDC-based shadow comparison ★
│   ├── src/audit/semantic.py           ← 6 critical business logic paths ★
│   ├── src/redaction/engine.py         ← HMAC-SHA256 PII redaction ★
│   ├── src/finops/arbitrator.py        ← Deadline-aware cost optimization ★
│   └── tests/ (40 tests)
│
├── phase5-cutover/                     ← PONR monitor, pre-flight, override gate, cutover
│   ├── src/override/gate.py            ← RS256 JWT operator gate — PONR block is absolute ★
│   ├── src/cutover/sequence.py         ← 7-step atomic cutover (<100ms downtime) ★
│   └── tests/ (48 tests)
│
└── phase6-hardening/                   ← Decommission, retraining, SRE report, runbooks
    ├── src/decommission/coordinator.py  ← 10-day staged teardown + deletion lock
    ├── src/retraining/engine.py         ← Cloud-native model retraining ★
    └── tests/ (35 tests)
```

★ = novel engineering not available in any existing migration tool

---

## Quick start

```bash
# Clone and spin up full stack
git clone https://github.com/your-org/migratex
cd migratex
docker compose up -d

# Run all tests (no live infra required)
for phase in phase1-discovery phase2-infrastructure phase3-chaos \
             phase4-dark-launch phase5-cutover phase6-hardening; do
    cd $phase && pip install -r requirements.txt -q && pytest tests/ -q && cd ..
done

# Run a phase individually
cd phase1-discovery
cp config/.env.example config/.env  # fill in DB credentials
python main.py run --ponr-data-gb 100 --ponr-write-mbps 50

# Simulate cutover (Phase 5)
cd ../phase5-cutover
python main.py generate-test-token
python main.py execute --token <token>
```

---

## Build order

| Phase | Timeline | Entry gate | Key output |
|-------|----------|-----------|-----------|
| **1 — Discovery** | Weeks 1–2 | Project kickoff | PONR engine, ML baseline, schema artifacts |
| **2 — Infrastructure** | Weeks 3–4 | Phase 1 signed off | CDC pipeline, GTID sink, etcd, snapshot |
| **3 — Chaos** | Weeks 5–6 | Phase 2 signed off | Resilience score ≥ 95, 7/7 experiments |
| **4 — Dark Launch** | Weeks 7–9 | Score ≥ 95 | Zero Class A over 72h, PII clean |
| **5 — Cutover** | Week 10 | Phase 4 signed off | Cloud authoritative, <100ms downtime |
| **6 — Hardening** | Weeks 11–12 | 24h validation | Edge decommissioned, runbooks delivered |

---

## Total test coverage: 225 tests — all pass without live infrastructure

---

## Non-negotiable rules

1. No cutover without signed operator JWT
2. No PONR override — P95 ≥ SLA = cutover blocked, no exceptions
3. No Merkle override — divergence > 0.01% blocks phase advancement
4. No edge termination before Day 10 — infrastructure deletion lock mandatory
5. Kafka 7-day retention floor — dead-man's-switch at 4 days of lag
6. Chaos blast radius bounded — source primary never touched

---

## Technology stack

| Concern | Technology |
|---------|-----------|
| PONR simulation | Python + NumPy (custom Monte Carlo) |
| Anomaly detection | scikit-learn Isolation Forest |
| CDC pipeline | Debezium + Kafka |
| Schema migrations | gh-ost |
| Distributed locking | etcd (Raft / Chubby lineage) |
| PII redaction | HMAC-SHA256 (Python `hmac`) |
| Operator auth | JWT RS256 |
| Observability | Prometheus + Grafana |
| API / CLI | FastAPI + Typer |
| Local orchestration | Docker Compose |
