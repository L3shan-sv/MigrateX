# Phase 4 Technical Documentation

## Architecture

```
Edge source (authoritative)
        │
        │ write commits
        ▼
Shadow Write Proxy
        ├── Wait for CDC propagation (configurable ms)
        ├── Query edge: SELECT * WHERE pk = ?  → edge_state
        ├── Query target: SELECT * WHERE pk = ? → target_state
        ├── Classify divergence (Class A/B/C/None)
        ├── Record to audit log
        └── Fire callback if Class A (autonomous pause)

Semantic Audit Layer (6 critical paths)
        ├── PATH-001: Booking integrity
        ├── PATH-002: User state transitions
        ├── PATH-003: Inventory availability
        ├── PATH-004: Review aggregation
        ├── PATH-005: Price calculation
        └── PATH-006: Search ranking

HMAC Redaction Engine
        ├── Load pii_surface_map.json (Phase 1 artifact)
        ├── Load fk_dependency_graph.json (Phase 1 artifact)
        ├── Process tables in FK dependency order
        └── HMAC-SHA256 all Tier 1 + Tier 2 columns

FinOps Arbitrator
        ├── Per-write classify: CRITICAL vs NON_CRITICAL
        ├── Check: deadline approaching?  → DEADLINE_OVERRIDE
        ├── Check: price spike?           → BUFFER_TO_WAL
        └── Check: normal price?          → FLOW_IMMEDIATELY
```

---

## Divergence classification algorithm

```python
def _classify(operation, edge_state, target_state, edge_ms, target_ms):
    if operation == "DELETE":
        if edge is None and target is None:  → NONE
        if edge is None and target exists:   → CLASS_A (not deleted on target)
        if edge exists and target is None:   → CLASS_B (CDC lag?)

    else:  # INSERT or UPDATE
        if both None:                        → CLASS_B (possible rollback)
        if edge exists, target None:         → CLASS_A (missing from target)
        if edge None, target exists:         → CLASS_B (unexpected)
        if both exist:
            diff = deep_diff(edge, target)
            if diff:                         → CLASS_A
            # NULL vs "" is normalized to None → not CLASS_A

    if target_ms > edge_ms × 2:             → CLASS_C (timing)
    else:                                    → NONE
```

### NULL vs empty string handling

NULL and empty string are treated as equivalent during comparison.
A column that is `NULL` on edge and `""` on target is **not** a Class A divergence.
This handles the common MySQL vs cloud MySQL collation difference.
It is logged as a Class B divergence for operator awareness.

---

## HMAC key management

The HMAC key is a 32-byte cryptographic secret generated per migration session.

**What is stored:**
- `session.key_fingerprint` — first 8 chars of `SHA256(key)` — for audit trail

**What is NOT stored:**
- The key itself is never written to disk, config files, or logs
- In production: store in AWS Secrets Manager / GCP Secret Manager / HashiCorp Vault

**Key rotation:**
- A new key is generated for each migration session (not each run of `main.py`)
- The key is consistent for the entire Phase 4–5 window
- After Phase 6, the key is revoked in the secrets manager

**Why this matters:**
If an attacker gets the raw HMAC output AND the key, they can verify guesses.
Without the key, a 64-character hex string gives them nothing.

---

## FinOps write classification

```
CRITICAL (never buffered):          NON_CRITICAL (bufferable):
  bookings                            analytics_events
  booking_payments                    page_views
  booking_status                      search_logs
  users                               audit_logs
  user_accounts                       activity_logs
  user_payments                       deleted_records
  payments                            soft_deletes
  payment_transactions                email_queue
  payment_methods                     notification_queue
  reservations
  holds
  [any unknown table]                 [only explicitly listed tables]
```

The safe default is CRITICAL. If a table is not in the NON_CRITICAL list, it is treated as critical. This means an unknown table is never accidentally buffered.

---

## 72-hour observation window

The window tracks:
- `window_start` — resets on every Class A divergence
- `hours_elapsed` — time since last Class A (or since window opened)
- `exit_criteria_met` — True only when `hours_elapsed >= 72` AND `class_a_count == 0`

The clock restarting on Class A is intentional and critical.
A migration that has zero Class A errors for 71 hours, then one Class A error,
must run for another 72 hours before the exit criterion is met.
There is no partial credit.

---

## Test coverage

```
tests/test_phase4.py
├── TestShadowWriteProxy (11 tests)
│   ├── Identical rows → no divergence
│   ├── Different values → Class A
│   ├── Class A restarts 72h window
│   ├── Missing target row → Class A (INSERT)
│   ├── Both deleted → no divergence
│   ├── Deleted on edge, persists on target → Class A
│   ├── High target latency → Class C
│   ├── Critical path flag correct
│   ├── Non-critical path flag correct
│   ├── Metrics track correctly
│   └── NULL vs empty string is not Class A
│
├── TestSemanticAuditLayer (5 tests)
│   ├── All 6 paths initialised
│   ├── Clean comparisons leave paths green
│   ├── Class A marks path failed
│   ├── Report shows blocking paths
│   └── Exit criteria false with Class A
│
├── TestHMACRedactionEngine (13 tests)
│   ├── Tier 1 column is redacted
│   ├── Tier 2 column is redacted
│   ├── Tier 3 passes through
│   ├── Non-PII passes through
│   ├── None value returns None
│   ├── HMAC is deterministic (same input)
│   ├── Different inputs produce different hashes
│   ├── redact_row redacts PII columns
│   ├── verify_determinism passes
│   ├── verify_irreversibility passes
│   ├── FK processing order parents before children
│   ├── Different keys produce different hashes
│   └── Session has fingerprint not key
│
└── TestFinOpsArbitrator (11 tests)
    ├── Critical table always flows
    ├── Non-critical buffered during spike
    ├── Non-critical flows at normal price
    ├── Deadline override suspends cost opt
    ├── WAL buffer accumulates buffered writes
    ├── Flush WAL clears buffer
    ├── Savings accumulate during buffering
    ├── classify_write: critical tables
    ├── classify_write: non-critical tables
    ├── Unknown table defaults to critical
    └── Meter tracks bytes and cost
```

Run: `pytest tests/ -v --tb=short`
