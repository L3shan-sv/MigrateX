# MigrateX — Phase 6: Post-Migration Hardening

> **Status:** Entry gate — Phase 5 24-hour extended validation passed. Edge decommission safe.
> **Timeline:** Weeks 11–12
> **Entry gate:** Phase 5 24h validation passed. `edge_decommission_safe = True`.
> **Exit gate:** Edge terminated. Models retrained. SRE report published. Runbooks accepted.

---

## What this phase does

The migration is complete. Phase 6 closes the loop professionally:

1. **Decommissions the edge source** safely over 10 days with orphan detection and a deletion lock
2. **Retrains both ML models** on cloud-native behavior (the Phase 1 models were trained on edge)
3. **Generates the SRE post-mortem report** — the authoritative record of the migration
4. **Produces 4 operational runbooks** the platform team inherits

---

## Quick start

```bash
pip install -r requirements.txt
pytest tests/ -v

# Full pipeline
python main.py run

# Or individually:
python main.py decommission --days-since-cutover 1
python main.py retrain
python main.py report --migration-id mig-001 --downtime-ms 87
python main.py runbooks
```

---

## 10-day decommission protocol

| Days | Action | Blocking? |
|------|--------|-----------|
| 1–3 | Edge online read-only. Write attempts logged. | Yes — any write = investigate |
| 4–7 | Remove all connection strings from app config. Read replicas offline. | Yes — orphan scan must find zero |
| 8–9 | Final backup + binlog archive (90 days) | Yes — backup must complete |
| 10 | Remove deletion lock (2-person sign-off) → terminate instance | Yes — all above must be complete |
| +30d | Delete archived storage volumes | Automated |

**The deletion lock is a hard rule.** It is applied at the infrastructure level and requires 2-operator sign-off to remove. This is not a policy — it is a technical control.

---

## Model retraining

The Isolation Forest was trained on edge behavior in Phase 1. Cloud-native patterns differ:
- Lower P99 latency (SSD-backed cloud storage vs on-prem SAN)
- Different connection pool dynamics
- Lower replication lag (internal VPC vs WAN)

The retrained model is saved to `artifacts/model/isolation_forest_cloud.pkl`. The Phase 1 model is archived — it remains the reference for edge behavior but must not be used for cloud operations.

The PONR engine is recalibrated with cloud-internal network parameters (10 Gbps vs 1 Gbps, 0.5ms RTT vs 5ms RTT). Post-recalibration, the engine models cloud→cloud DR failover, not edge→cloud migration.

---

## The 4 operational runbooks

| ID | Title | Owner |
|----|-------|-------|
| RB-001 | Cloud DB health | Platform SRE |
| RB-002 | Anomaly response | Platform SRE |
| RB-003 | DR failover | Platform SRE |
| RB-004 | MigrateX operational reference | Platform SRE |

---

## Phase 6 exit criteria

- [ ] Edge source terminated (Day 10 checklist complete)
- [ ] Zero unresolved orphan references
- [ ] Isolation Forest retrained on 14-day cloud-native baseline
- [ ] PONR engine recalibrated — P50 within 10% of observed cost
- [ ] SRE report published and reviewed by stakeholders
- [ ] All 4 runbooks reviewed and accepted by platform team
- [ ] Final financial reconciliation published
- [ ] Project retrospective completed
