# MigrateX — Phase 3: Dry Run & Chaos Validation

> **Status:** Entry gate — Phase 2 exit criteria must be signed off before this phase begins.
> **Timeline:** Weeks 5–6
> **Entry gate:** Phase 2 exit criteria signed off (CDC lag < 30s, etcd quorum healthy, synthetic validation passed)
> **Exit gate:** Resilience score > 95. All 7 experiments passed. Merkle clean post-chaos.

---

## What this phase does

Phase 3 proves the system can survive worst-case scenarios before they happen in production. It does this by:

1. Replaying 5x production traffic volume against the live CDC pipeline for 48 hours
2. Systematically injecting 7 failure modes — each with a defined hypothesis and pass criteria
3. Computing a 100-point resilience score from the results
4. Blocking Phase 4 if the score is below 95

**The 95-point gate has no operator override. If the score is below 95, failing components are diagnosed and fixed, and the full experiment suite is re-run.**

---

## Quick start

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests (no live infrastructure required)
pytest tests/ -v

# Run all 7 experiments (simulation mode — no live infra)
python main.py run-experiments

# Calculate resilience score from saved results
python main.py score

# Full pipeline
python main.py run
```

---

## The 7 chaos experiments

| ID | Name | Blast radius | Pass criteria |
|----|------|-------------|---------------|
| EXP-001 | Network blackhole (5 min) | Edge↔Cloud network | Merkle = 0 within 5 min, lag < 30s |
| EXP-002 | CDC consumer process kill | Shadow traffic only | Restart < 10s, idempotent replay |
| EXP-003 | State drift injection (100 rows) | Target DB only | Merkle detects < 10 min, auto-repair |
| EXP-004 | etcd leader kill | etcd cluster | Election < 2s, writes resume < 3s |
| EXP-005 | Schema migration under load | Source schema | Zero locks, lag stable, schemas match |
| EXP-006 | Kafka broker failure | Kafka cluster | Reassignment < 30s, zero message loss |
| EXP-007 | PONR under degraded network | Edge↔Cloud network | PONR blocks cutover, alert < 2 min |

---

## Resilience score breakdown

| Component | Max points | Scoring |
|-----------|-----------|---------|
| Performance benchmarks (6/6 pass) | 30 pts | 5 pts each |
| Chaos experiments (7/7 pass) | 50 pts | ~7.14 pts each |
| PONR model stability | 10 pts | Full if within 20% of Phase 1 estimate |
| Anomaly detection accuracy | 10 pts | Full if all 3 calibration modes detected |
| **Total** | **100 pts** | **Must exceed 95 to proceed** |

---

## Blast radius rules (non-negotiable)

- All fault injection operates on the CDC consumer, network path, Kafka, or etcd **only**
- The source database **primary is never touched**
- Max fault duration: **10 minutes** — auto-recovery fires at timeout regardless
- Experiments are **disabled** if production anomaly score > 0.3 (chaos never compounds a real incident)

---

## Phase 3 exit criteria

- [ ] 5x traffic replay completed for 48 hours with no P1 alerts
- [ ] All 6 performance benchmarks passed at 5x load
- [ ] All 7 chaos experiments: PASSED state
- [ ] Resilience score ≥ 95 (no operator override for this gate)
- [ ] PONR model validated under degraded network (EXP-007)
- [ ] Merkle full verification clean post-chaos (zero divergence)
