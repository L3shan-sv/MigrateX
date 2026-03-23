# Phase 3 Technical Documentation

## Architecture

```
5x Traffic Replay
  ├── Binlog capture (24h peak window)
  ├── Replay at 5x speed against read replica
  ├── Monitor: lag, P99, Merkle, PONR, etcd health
  └── Auto-throttle to 3x if lag > 120s

Chaos Experiment Suite (7 experiments)
  ├── Pre-flight: anomaly_score < 0.3 (never compound real incidents)
  ├── Inject fault → bounded blast radius
  ├── Wait (max 10 min) → monitor for production anomalies
  ├── Verify recovery → check pass criteria
  ├── Cleanup (always, even on failure)
  └── Record result → resilience scorer

Resilience Scorer
  ├── Benchmark score (30 pts): 6 metrics × 5 pts
  ├── Chaos score (50 pts): 7 experiments × 7.14 pts
  ├── PONR stability (10 pts): deviation from Phase 1 estimate
  ├── Anomaly accuracy (10 pts): calibration check
  └── Total ≥ 95 → PROCEED | < 95 → BLOCKED (no override)
```

---

## Experiment lifecycle state machine

```
PENDING
  │
  ▼
[Pre-flight: anomaly_score check]
  │
  ├── score > 0.3 → SKIPPED
  │
  ▼
RUNNING
  │
  ├── inject_fault()
  ├── _wait_with_monitoring() [max_fault_duration_seconds]
  │     └── early exit if anomaly_score > 0.5 during experiment
  ├── verify_recovery() → bool
  ├── check_pass_criteria() → list[dict]
  └── cleanup() [always called]
  │
  ├── all criteria passed → PASSED
  ├── any criterion failed → FAILED
  └── exception → ABORTED
```

---

## Scoring formula

```python
benchmark_score = passed_benchmarks × (30 / 6)          # 5.0 pts each
chaos_score = passed_experiments × (50 / 7)              # ~7.14 pts each
ponr_score = 10.0 if deviation <= 20% else
             5.0  if deviation <= 40% else 0.0
anomaly_score = 10.0 if calibration_passed else 0.0

total = round(benchmark_score + chaos_score + ponr_score + anomaly_score)
passed = total >= 95
```

---

## 5x Replay throttle logic

```
if consumer_lag > 120s:
    multiplier = 3x
    fire MEDIUM alert
    log throttle event

if consumer_lag < 60s and was_throttled:
    restore multiplier = 5x
    log recovery
```

The throttle preserves the validity of the benchmark measurements.
A lag spike caused by the replay itself (not a real problem) is managed
by the throttle — not by declaring the benchmark failed.

---

## Blast radius boundaries

Each experiment is categorised by what it touches:

| BlastRadius | What is affected | What is NOT affected |
|-------------|-----------------|---------------------|
| `SHADOW_ONLY` | CDC consumer process, target DB (test tables only) | Source primary, production tables |
| `NETWORK_PATH` | Edge↔Cloud network path | Source primary, Kafka, etcd |
| `ETCD_ONLY` | etcd nodes | Source primary, Kafka, CDC pipeline |
| `KAFKA_ONLY` | Kafka brokers | Source primary, etcd, source DB |
| `SCHEMA_ONLY` | DDL via gh-ost on test table | Production tables, CDC pipeline data |

---

## Test coverage

```
tests/test_phase3.py
├── TestResilienceScorer (10 tests)
│   ├── Perfect score is 100
│   ├── Score above 95 passes
│   ├── Score exactly 95 passes
│   ├── All experiments failed gives 50 pts max
│   ├── Blocking failures listed when failed
│   ├── High PONR deviation reduces score
│   ├── Anomaly failure deducts 10 points
│   ├── Report saves to file correctly
│   ├── Partial benchmark pass scores proportionally
│   └── Recommendation contains correct keyword
│
├── TestBenchmarkEvaluation (7 tests)
│   ├── All passing snapshot produces 6 passes
│   ├── Lag above threshold fails benchmark
│   ├── High insert latency fails
│   ├── Merkle at threshold passes
│   ├── Merkle above threshold fails
│   ├── etcd below threshold fails
│   └── Correct number of benchmarks defined (6)
│
├── TestChaosExperimentBase (5 tests)
│   ├── High anomaly score skips experiment
│   ├── Blast radius correct for each experiment
│   ├── Each experiment has non-empty hypothesis
│   ├── Experiment IDs are unique
│   └── Total score weight sums to 50
│
├── TestTrafficReplayEngine (4 tests)
│   ├── Throttle activates when lag exceeds threshold
│   ├── Throttle restores on lag recovery
│   ├── Benchmark snapshot has all fields
│   └── PONR stability zero when no Phase 1 estimate
│
├── TestPONRDegradedExperiment (3 tests)
│   ├── Experiment blocks when PONR signals BLOCK
│   ├── Experiment has 3 pass criteria
│   └── All criteria pass when PONR blocked and alert fast
│
└── TestNetworkBlackholeExperiment (3 tests)
    ├── Recovery check fails on high lag
    ├── Recovery check passes on clean state
    └── No duplicate writes criterion always passes
```

Run: `pytest tests/ -v --tb=short`
