# Phase 5 Technical Documentation

## Architecture

```
T-48h: PreflightChecklist.run()
  ├── PF-001..006: automated (lag, PONR, anomaly, Merkle, etcd, Kafka)
  └── PF-007..010: manual operator sign-off

T-2h: LivePONRMonitor.enter_event_horizon_mode()
  └── Monte Carlo at 30s intervals
      ├── PROCEED → cutover permitted
      ├── CAUTION → prepare Tier 3 sequence
      └── BLOCK   → gate locked, GO rejected

T-30m: Final Call notification
  └── Full PONR distribution + system recommendation sent to operator

T-0: OperatorOverrideGate.submit_go(jwt)
  ├── Validate JWT (RS256/HS256)
  ├── Check PONR not blocked
  └── Accept → AtomicCutoverSequence.execute()

AtomicCutoverSequence:
  Step 1: Drain queue (50ms buffer)
  Step 2: Merkle verify (top 10 tables)  ← abort: divergence
  Step 3: etcd lease (cloud epoch N+1)   ← abort: failure / rtt / anomaly
  Step 4: DNS swing (TTL=1s)             ← abort: anomaly / DNS failure
  Step 5: Flush buffer                   ← abort: anomaly
  Step 6: edge SET read_only=1           ← no abort (log only)
  Step 7: Full Merkle verify             ← Tier 4 if fails

T+0 to T+30m: PostCutoverMonitor immediate window
  └── 10s checks: P99, error rate, Merkle, anomaly, pool utilisation

T+30m to T+24h: PostCutoverMonitor extended window
  └── Merkle every 15min (2h), hourly (rest), spot checks every cycle
```

---

## PONR simulation (live mode)

The same Monte Carlo model from Phase 1, running against live metrics:

```
cost = egress_cost + resync_cost + downtime_cost + merkle_repair_cost

egress_cost       = bytes_transferred_gb × $0.09/GB
resync_cost       = (lag_s × write_rate_bytes/s / 1e9) / rollback_tp × $5,000/h
downtime_cost     = (60s + resync_s) / 3600 × $5,000/h
merkle_repair     = (since_merkle_s × write_rate × 0.001 / 1e9) / rollback_tp × $5,000/h

rollback_tp ~ Normal(0.5 Gbps, σ/2)   sampled 10,000× per evaluation
```

State transitions:
```
PROCEED  → P95 < SLA × 0.80   → cutover safe
CAUTION  → P95 ∈ [SLA×0.80, SLA)  → prepare Tier 3, continue monitoring
BLOCK    → P95 ≥ SLA           → gate locked, operator must wait
```

Transitions fire callbacks. BLOCK fires PagerDuty critical alert immediately.

---

## JWT token specification

```json
{
  "sub": "operator@company.com",
  "iat": 1234567890,
  "exp": 1234567890 + 1500,
  "migratex_action": "CUTOVER",
  "cutover_window": "2026-01-15T02:00:00",
  "ponr_p95_at_signing": 1234.56,
  "replication_lag_at_signing": 2.3,
  "anomaly_score_at_signing": 0.12,
  "jti": "unique-token-id-hex"
}
```

**Production:** RS256 signed with operator's private key. Public key on MigrateX server.
**Development/CI:** HS256 with shared secret `"test-secret"`. Never use in production.

The `jti` (JWT ID) prevents token replay attacks — each token can only be used once.
The `ponr_p95_at_signing` field means a token signed during a low-risk window
cannot be used if conditions deteriorate — the BLOCK check enforces this.

---

## Abort vs rollback distinction

| Scenario | What happens | Who acts |
|----------|-------------|---------|
| Abort at Steps 1–5 | Sequence halts. Edge stays authoritative. | Orchestrator |
| Step 6 set (read_only=1) then Step 7 fails | Tier 4 rollback: release lease, un-readonly edge, Merkle repair | SRE + IC |
| 24h extended validation fails | Tier 3 or 4 depending on PONR P95 | IC decision |

The edge source stays online read-only for 24 hours after cutover precisely to preserve the Tier 3 rollback path. Do not terminate it early.

---

## Connection pool drain — implementation requirement

This is a pre-build contract gap that must be implemented in the application:

```python
# Application DB access layer
MAX_RETRIES = 2

def execute_write(query, params, retries=0):
    try:
        return connection_pool.execute(query, params)
    except MySQLError as e:
        if e.errno == 1290 and retries < MAX_RETRIES:
            # MySQL errno 1290 = read_only mode
            # Edge is now read-only → force reconnect → cloud DNS
            logger.warning("DB read_only error — forcing pool reconnect")
            connection_pool.close_all()
            connection_pool.reconnect()
            return execute_write(query, params, retries + 1)
        raise
```

Pre-flight item PF-010 verifies that the application connection pool max idle time is set to 30 seconds. This ensures most connections have cycled before T-0, minimising the number of connections that will hit the 1290 error path.

---

## Test coverage

```
tests/test_phase5.py
├── TestLivePONRMonitor (11 tests)
│   ├── Evaluate returns snapshot with correct ordering
│   ├── PROCEED when P95 well below SLA
│   ├── BLOCK when P95 above SLA
│   ├── CAUTION fires callback on transition
│   ├── BLOCK fires callback on transition
│   ├── History accumulates over evaluations
│   ├── is_cutover_permitted: True when PROCEED
│   ├── is_cutover_permitted: False when BLOCKED
│   ├── Network health high for stable network
│   ├── Network health low for unstable network
│   └── Event horizon = 0 when already blocked
│
├── TestPreflightChecklist (10 tests)
│   ├── All automated green with good metrics
│   ├── High lag → RED PF-001
│   ├── PONR above SLA → RED PF-002
│   ├── High anomaly → RED PF-003
│   ├── Stale Merkle → RED PF-004
│   ├── Manual items start as MANUAL
│   ├── Sign-off turns MANUAL to GREEN
│   ├── All green requires all sign-offs
│   ├── Result has 10 items
│   └── Blocking items listed when red
│
├── TestOperatorOverrideGate (8 tests)
│   ├── Valid test token → GO decision
│   ├── PONR BLOCK → GO rejected
│   ├── Invalid token → rejected
│   ├── POSTPONE decision recorded
│   ├── ABORT decision recorded
│   ├── All decisions in audit trail
│   ├── PONR unblock allows GO again
│   └── Token contains PONR context
│
├── TestAtomicCutoverSequence (12 tests)
│   ├── Successful cutover → COMPLETED
│   ├── Successful cutover has 7 steps
│   ├── Merkle divergence → ABORT at Step 2
│   ├── etcd failure → ABORT at Step 3
│   ├── High anomaly → ABORT at Step 3
│   ├── Degraded network → ABORT at Step 3
│   ├── Fencing epoch recorded on success
│   ├── Post-Merkle failure → rolled_back state
│   ├── Step callbacks fire for each step
│   ├── Abort callback fires on abort
│   ├── Duration under 5000ms on success
│   └── Operator and token ID in result
│
└── TestPostCutoverMonitor (7 tests)
    ├── Immediate check all green
    ├── High error rate fails check
    ├── High P99 fails check
    ├── Merkle check clean
    ├── Merkle check dirty
    ├── Generate report all fields
    └── Edge decommission requires all passed
```

Run: `pytest tests/ -v --tb=short`
