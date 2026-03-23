# MigrateX — Phase 4: Dark Launch & Semantic Audit

> **Status:** Entry gate — Phase 3 resilience score > 95 required.
> **Timeline:** Weeks 7–9
> **Entry gate:** Phase 3 resilience score > 95. All 7 chaos experiments passed.
> **Exit gate:** Zero Class A divergences over 72-hour window. PII redaction clean. Shadow error rate < 0.01%.

---

## What this phase does

Phase 4 answers the hardest question in the migration:

> *Does the cloud target think the same way the edge source does?*

Row-count parity is not enough. Two databases can have identical rows and still produce different results if their configuration, stored logic, or query execution plans differ. Phase 4 uses shadow writes — routing real production traffic to both databases and comparing results — to catch these differences before the edge source is decommissioned.

**Shadow writes work via CDC (Option C — safest approach):**  
The target already receives all writes via Debezium CDC from Phase 2. The shadow proxy intercepts each edge write AFTER it commits, queries BOTH databases for the affected row, and compares the result. The application always sees edge responses — the cloud is never in the critical path during Phase 4.

---

## Divergence classification

| Class | Definition | Action |
|-------|------------|--------|
| **Class A** — Hard | Different row values after write | Autonomous pause + operator notification + **72h clock restarts** |
| **Class B** — Soft | Different execution plan, NULL handling, ordering | Logged, operator review required, does not block |
| **Class C** — Timing | Same result but target P99 > 2× edge | Logged, capacity review, does not block |

**Exit criterion: zero Class A divergences over 72 continuous hours across all 6 critical business logic paths.**

---

## Quick start

```bash
pip install -r requirements.txt

# Run tests (no live DB required)
pytest tests/ -v

# PII redaction validation
python main.py check-pii

# FinOps status
python main.py check-finops

# Full Phase 4 status
python main.py run
```

---

## The 6 critical business logic paths

| Path | What is checked |
|------|----------------|
| PATH-001: Booking transaction integrity | Full booking workflow state — identical on edge and target |
| PATH-002: User account state transitions | Create, verify, suspend, delete → identical row state |
| PATH-003: Inventory availability | Availability queries → identical result sets |
| PATH-004: Review aggregation | AVG(rating), COUNT(*) → identical aggregates |
| PATH-005: Price calculation | Pricing views/procs → identical outputs |
| PATH-006: Search ranking | Row identity set identical (order is Class B) |

---

## PII Redaction

All data leaving the local environment passes through HMAC-SHA256 redaction:

- **Not plain SHA256** — reversible via rainbow table for low-entropy values like emails
- **HMAC with rotating key** — keyed hash, not reversible without the key
- **Consistent within migration run** — same plaintext = same HMAC, preserving FK integrity
- **Processing order** — FK dependency graph from Phase 1 defines parent-before-child order

**Phase 4 exit criteria for PII:**
- Determinism test: 3× same input = same output ✓
- Irreversibility test: hash output is not PII format ✓  
- FK constraint replay: zero violations after redaction ✓

---

## FinOps Arbitrator

Activated at start of Phase 4:

```
Critical writes (bookings, payments, users)  → Always flow immediately
Non-critical writes (analytics, logs)        → Buffer to WAL if price > 1.5× baseline
Deadline within 24h                          → Cost optimization suspended entirely
WAL age > 2 hours                            → Class C alert + auto-drain
```

---

## Phase 4 exit criteria

- [ ] Zero Class A divergences over 72-hour window (any Class A restarts clock)
- [ ] All Class B divergences documented with root cause and resolution
- [ ] All 6 critical business logic paths at zero Class A error rate
- [ ] PII redaction determinism and irreversibility tests passed
- [ ] Referential integrity after redaction: zero FK violations
- [ ] FinOps cost tracking active, projection within P95 estimate
- [ ] Shadow write error rate < 0.01% across all write operations
