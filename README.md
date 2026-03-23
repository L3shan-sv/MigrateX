# MigrateX — The Autonomous Data Fabric

> Risk-quantified, zero-downtime database migration from on-premise to cloud at Airbnb scale.

[![Phase 1](https://img.shields.io/badge/Phase%201-Discovery-blue)]()
[![Phase 2](https://img.shields.io/badge/Phase%202-Infrastructure-gray)]()
[![Phase 3](https://img.shields.io/badge/Phase%203-Chaos-gray)]()
[![Phase 4](https://img.shields.io/badge/Phase%204-Dark%20Launch-gray)]()
[![Phase 5](https://img.shields.io/badge/Phase%205-Cutover-gray)]()
[![Phase 6](https://img.shields.io/badge/Phase%206-Hardening-gray)]()

---

## What is MigrateX?

MigrateX is a production-grade database migration orchestrator built to answer the question that no existing tool answers:

> *At what exact point does rolling back become more dangerous than pressing forward?*

It replaces the midnight gut call with a Monte Carlo probability model. It replaces "I think it's fine" with P5/P50/P95 rollback cost distributions updated every 60 seconds.

**Designed for:** 100TB+ operational databases, globally distributed traffic, 99.99% availability SLAs.
**Reference scenario:** Migrating an Airbnb-scale MySQL cluster from on-premise edge infrastructure to cloud.

---

## Core guarantees

| Guarantee | How it is enforced |
|-----------|-------------------|
| Zero data loss | Merkle tree O(log N) dirty-block detection + targeted repair |
| Zero double writes | GTID-based atomic deduplication in the sink consumer |
| Zero downtime | Atomic etcd lease transfer + connection pool drain (<100ms swing) |
| Quantified rollback risk | Monte Carlo PONR engine — P95 gate blocks cutover |
| No silent data divergence | Semantic audit layer — Class A/B/C divergence classification |
| Human always in the loop | Signed JWT operator token required at every cutover gate |

---

## Project structure

```
migratex/
├── docker-compose.yml              ← spins up the entire stack
├── README.md                       ← you are here
├── DOCUMENTATION.md                ← root technical reference
│
├── phase1-discovery/               ← Schema scan, traffic profile, PONR init, ML baseline
│   ├── README.md
│   ├── DOCUMENTATION.md
│   ├── requirements.txt
│   ├── main.py
│   ├── src/
│   │   ├── scanner/schema.py       ← schema inventory + PII surface mapping
│   │   ├── profiler/traffic.py     ← QPS, P99, write amplification
│   │   ├── ponr/engine.py          ← Monte Carlo PONR simulation engine
│   │   ├── ml_baseline/anomaly.py  ← Isolation Forest anomaly detector
│   │   ├── db.py                   ← thread-safe MySQL connection pool
│   │   └── settings.py             ← pydantic settings from .env
│   └── tests/test_phase1.py
│
├── phase2-infrastructure/          ← CDC pipeline, etcd cluster, observability [NEXT]
├── phase3-chaos/                   ← 7-experiment chaos suite + resilience scoring [NEXT]
├── phase4-dark-launch/             ← Shadow writes + semantic audit [NEXT]
├── phase5-cutover/                 ← Event Horizon monitor + atomic cutover [NEXT]
└── phase6-hardening/               ← Decommission + model retraining + SRE report [NEXT]
```

---

## Quick start

### Prerequisites
- Docker + Docker Compose
- Python 3.11+
- 8GB RAM minimum for the full stack

### Start the full stack

```bash
# Clone and start everything
git clone https://github.com/your-org/migratex
cd migratex
docker compose up -d

# Wait for source and target MySQL to be healthy
docker compose ps

# Run Phase 1 discovery
docker compose logs -f phase1
```

### Phase 1 only (fastest path to first output)

```bash
cd phase1-discovery
pip install -r requirements.txt
cp config/.env.example config/.env
# Edit config/.env with your DB credentials

# Run full discovery pipeline
python main.py run --ponr-data-gb 100 --ponr-write-mbps 50

# Or run tests without a DB
pytest tests/ -v
```

---

## Build order

Phases are built and deployed independently. Each phase is fully tested before the next begins.

| Phase | Status | What it builds |
|-------|--------|----------------|
| **Phase 1 — Discovery** | ✅ Built | PONR engine, Isolation Forest, schema scanner, traffic profiler |
| **Phase 2 — Infrastructure** | 🔲 Next | Debezium CDC, Kafka sink consumer, GTID deduplication, etcd lease |
| **Phase 3 — Chaos** | 🔲 | 7-experiment chaos suite, resilience scorer, 5x traffic replay |
| **Phase 4 — Dark Launch** | 🔲 | Shadow write proxy, semantic audit layer, FinOps arbitrator |
| **Phase 5 — Cutover** | 🔲 | Event Horizon monitor, atomic cutover sequence, override gate |
| **Phase 6 — Hardening** | 🔲 | Decommission protocol, model retraining, SRE report generator |

---

## Technology stack

| Concern | Technology | Why |
|---------|-----------|-----|
| PONR simulation | Python + NumPy | Custom Monte Carlo — no existing tool does this |
| Anomaly detection | scikit-learn Isolation Forest | Unsupervised, no labelled anomaly data needed |
| CDC pipeline | Debezium + Kafka | Battle-tested, Kafka-native, schema registry |
| Schema migrations | gh-ost | Zero-lock online DDL, proven at scale |
| Distributed locking | etcd (Raft) | Chubby lineage, linearizable, lease TTL |
| PII redaction | HMAC-SHA256 (Python hmac) | Keyed hash — not reversible via rainbow table |
| Observability | Prometheus + Grafana | Industry standard, multi-window burn-rate support |
| API layer | FastAPI | Async, typed, auto-docs |
| Orchestration | Docker Compose (dev) | Single-command full stack |

---

## Fallback plan

MigrateX has a four-tier incident response plan defined before any code is built. See `FALLBACK_PLAN.md` for the complete spec. The short version:

- **Tier 1:** Autonomous recovery — system self-heals, no human needed
- **Tier 2:** Operator-guided — SRE paged, playbook executed, migration resumes
- **Tier 3:** Controlled rollback — PONR P95 below threshold, clean rollback in <5 min
- **Tier 4:** Emergency rollback — PONR breached, Merkle repair from Kafka WAL

There is no Tier 5. Every scenario has a defined exit.

---

## Non-negotiable hard rules

1. **No code cutover without signed operator JWT.** The ML advises. The human decides.
2. **No edge source termination before T+10 days.** Infrastructure deletion lock is mandatory.
3. **No PONR override.** If P95 > SLA threshold, cutover is blocked. Full stop.
4. **No Merkle override.** Divergence > 0.01% blocks phase advancement. Always.
5. **Kafka retention floor of 7 days.** Dead-man's-switch alerts at 4 days of lag.

---

## License

Internal use. See `LICENSE` for terms.
