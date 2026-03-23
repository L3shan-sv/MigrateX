# MigrateX — Phase 5: Event Horizon & Atomic Cutover

> **Status:** Entry gate — Phase 4 exit criteria must be signed off.
> **Timeline:** Week 10
> **Entry gate:** Zero Class A divergences over 72h. PII redaction clean. Shadow error rate < 0.01%.
> **Exit gate:** Cloud target authoritative. Edge read-only. Post-Merkle clean. 24h extended validation passed.

---

## What this phase does

Phase 5 is the cutover. Every phase before this was preparation. This phase executes the atomic transfer of write authority from edge to cloud in under 100ms of application downtime.

It is structured as a T-minus countdown:

```
T-48h   Pre-flight checklist activates (10 items, 6 automated + 4 manual)
T-2h    PONR Event Horizon window opens (30-second simulation intervals)
T-30m   Final Call notification issued to operator
T-0     Operator submits signed JWT → 7-step atomic sequence begins
T+30m   Immediate validation window ends
T+24h   Extended validation window ends → Phase 6 safe to start
```

---

## Quick start

```bash
pip install -r requirements.txt

# Tests (no live infra)
pytest tests/ -v

# Run pre-flight checks
python main.py preflight

# Sign off manual items
python main.py signoff PF-007 --operator ops@company.com
python main.py signoff PF-008 --operator ops@company.com
python main.py signoff PF-009 --operator ops@company.com
python main.py signoff PF-010 --operator ops@company.com

# Issue Final Call (T-30min)
python main.py final-call

# Generate test token (dev only)
python main.py generate-test-token --operator ops@company.com

# Execute cutover
python main.py execute --token <jwt> --operator ops@company.com

# Postpone
python main.py postpone --reason NETWORK_DEGRADED --operator ops@company.com

# Abort
python main.py abort --reason "Critical incident in progress" --operator ops@company.com
```

---

## The 7-step atomic sequence

| Step | Name | Duration | Abort condition |
|------|------|---------|----------------|
| 1 | Drain write queue | 50ms | None |
| 2 | Final Merkle verification | < 30s | Any divergence > 0 |
| 3 | Cloud acquires etcd lease | < 2s | Lease failure, network RTT > 3×, anomaly spike |
| 4 | DNS swing to cloud | < 1s | Anomaly spike, DNS failure |
| 5 | Flush 50ms write buffer | 10ms | Anomaly spike |
| 6 | Set edge source read-only | < 1s | Log only (point of commitment) |
| 7 | Post-cutover Merkle (all tables) | < 5 min | Divergence found → Tier 4 required |

**Abort conditions only apply to Steps 1–5.** After Step 6, the edge is read-only and a Tier 4 rollback is required if Step 7 fails.

---

## Operator JWT token

The operator token is a signed JWT (RS256 in production, HS256 in test) that binds the cutover decision to:
- The operator's identity
- The PONR P95 value at the time of signing
- The cutover window timestamp
- A unique token ID

The token expires in 25 minutes. A token generated 30 minutes ago is rejected even if otherwise valid. The operator cannot sign a token for a different risk profile than the one they reviewed.

**PONR BLOCK cannot be overridden.** If P95 >= SLA threshold, the gate rejects all GO decisions regardless of the token.

---

## Connection pool drain (pre-build contract)

The zero-downtime guarantee requires this to be implemented in the application layer:

```python
# In the application's DB access layer:
try:
    connection.execute(write_query)
except MySQLError as e:
    if e.errno == 1290:  # read_only error
        # Edge is now read-only — force pool reconnection
        connection_pool.close_all()
        connection_pool.reconnect()  # resolves new DNS → cloud target
        connection.execute(write_query)  # retry on cloud
```

This is the safety net for stale connections that missed the DNS swing.

---

## Phase 5 exit criteria

- [ ] Cloud target holds authoritative etcd lease
- [ ] Edge source confirmed read-only (`read_only = 1`)
- [ ] Post-cutover Merkle verification: zero divergence
- [ ] Application error rate < 0.01% for 30 minutes post-cutover
- [ ] Cloud target P99 latency within 20% of Phase 3 baseline
- [ ] All connection pools reconnected and within capacity
- [ ] 24-hour extended validation: zero P1 alerts
