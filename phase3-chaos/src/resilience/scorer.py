"""
resilience/scorer.py
────────────────────
Resilience score calculator for Phase 3.

The resilience score is a single number from 0 to 100.
It must exceed 95 before Phase 4 is permitted to begin.
There is no operator override for this gate.

Scoring breakdown:
  Component                          Weight
  ─────────────────────────────────────────
  Performance benchmarks (6/6 pass)   30 pts  (5 pts each)
  Chaos experiments (7/7 pass)        50 pts  (~7.14 pts each)
  PONR model stability                10 pts
  Anomaly detection accuracy          10 pts
  ─────────────────────────────────────────
  Total                              100 pts

A score below 95 means at least one of the following:
  - A chaos experiment failed (the most common cause)
  - A performance benchmark was not met at 5x load
  - The PONR model drifted significantly from Phase 1 estimate
  - The anomaly detector missed a calibration failure mode
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Any

from src.chaos.base import ExperimentResult, ExperimentState

logger = logging.getLogger(__name__)


# ── Benchmark result ──────────────────────────────────────────────────────────

@dataclass
class BenchmarkResult:
    name: str
    metric: str
    threshold: str
    actual: str
    passed: bool
    weight_pts: float = 5.0


# ── Resilience report ─────────────────────────────────────────────────────────

@dataclass
class ResilienceReport:
    score: int
    passed: bool                    # True if score >= 95
    evaluated_at: str

    # Component scores
    benchmark_score: float          # max 30
    chaos_score: float              # max 50
    ponr_stability_score: float     # max 10
    anomaly_accuracy_score: float   # max 10

    # Detail
    benchmarks: list[BenchmarkResult]
    experiments: list[ExperimentResult]
    ponr_stability_pct: float       # % deviation from Phase 1 estimate
    anomaly_calibration_passed: bool

    # Verdict
    blocking_failures: list[str]    # experiments/benchmarks that failed
    recommendation: str


class ResilienceScorer:
    """
    Calculates the resilience score from benchmark and experiment results.
    This is the sole gate on Phase 4 entry.
    """

    PASS_THRESHOLD = 95
    BENCHMARK_TOTAL_PTS = 30
    CHAOS_TOTAL_PTS = 50
    PONR_TOTAL_PTS = 10
    ANOMALY_TOTAL_PTS = 10

    BENCHMARK_PTS_EACH = BENCHMARK_TOTAL_PTS / 6      # 5.0 each
    CHAOS_PTS_EACH = CHAOS_TOTAL_PTS / 7              # ~7.14 each

    def calculate(
        self,
        benchmarks: list[BenchmarkResult],
        experiments: list[ExperimentResult],
        ponr_deviation_pct: float,
        anomaly_calibration_passed: bool,
    ) -> ResilienceReport:
        """
        Calculate the final resilience score.

        Args:
            benchmarks: results from the 6 performance benchmarks
            experiments: results from all 7 chaos experiments
            ponr_deviation_pct: % deviation of live P95 from Phase 1 estimate
            anomaly_calibration_passed: True if all 3 failure modes detected
        """

        # ── Benchmark score (30 pts) ──────────────────────────────────────────
        passed_benchmarks = sum(1 for b in benchmarks if b.passed)
        benchmark_score = passed_benchmarks * self.BENCHMARK_PTS_EACH

        # ── Chaos score (50 pts) ─────────────────────────────────────────────
        passed_experiments = sum(
            1 for e in experiments
            if e.state == ExperimentState.PASSED
        )
        chaos_score = passed_experiments * self.CHAOS_PTS_EACH

        # ── PONR stability (10 pts) ───────────────────────────────────────────
        # Full 10 pts if P95 within 20% of Phase 1 estimate
        # Partial scoring for slight drift
        if ponr_deviation_pct <= 20.0:
            ponr_score = self.PONR_TOTAL_PTS
        elif ponr_deviation_pct <= 40.0:
            ponr_score = self.PONR_TOTAL_PTS * 0.5
        else:
            ponr_score = 0.0

        # ── Anomaly detection (10 pts) ────────────────────────────────────────
        anomaly_score = self.ANOMALY_TOTAL_PTS if anomaly_calibration_passed else 0.0

        # ── Total ─────────────────────────────────────────────────────────────
        total = benchmark_score + chaos_score + ponr_score + anomaly_score
        total_rounded = round(total)
        passed = total_rounded >= self.PASS_THRESHOLD

        # ── Blocking failures ─────────────────────────────────────────────────
        blocking = []
        for b in benchmarks:
            if not b.passed:
                blocking.append(f"Benchmark FAILED: {b.name} ({b.actual} vs threshold {b.threshold})")
        for e in experiments:
            if e.state not in (ExperimentState.PASSED, ExperimentState.SKIPPED):
                blocking.append(f"Experiment FAILED: {e.name} (state={e.state.value})")
        if not anomaly_calibration_passed:
            blocking.append("Anomaly detector failed calibration check")
        if ponr_deviation_pct > 40.0:
            blocking.append(f"PONR model drift too high: {ponr_deviation_pct:.1f}% > 40%")

        # ── Recommendation ────────────────────────────────────────────────────
        if passed:
            recommendation = (
                "PROCEED TO PHASE 4 — resilience score meets threshold. "
                "All exit criteria satisfied."
            )
        else:
            pts_needed = self.PASS_THRESHOLD - total_rounded
            recommendation = (
                f"BLOCKED — score {total_rounded}/100 is {pts_needed} points below threshold. "
                f"Fix the following failures and re-run the full experiment suite: "
                + "; ".join(blocking)
            )

        report = ResilienceReport(
            score=total_rounded,
            passed=passed,
            evaluated_at=datetime.utcnow().isoformat(),
            benchmark_score=round(benchmark_score, 2),
            chaos_score=round(chaos_score, 2),
            ponr_stability_score=round(ponr_score, 2),
            anomaly_accuracy_score=round(anomaly_score, 2),
            benchmarks=benchmarks,
            experiments=experiments,
            ponr_stability_pct=ponr_deviation_pct,
            anomaly_calibration_passed=anomaly_calibration_passed,
            blocking_failures=blocking,
            recommendation=recommendation,
        )

        self._log_report(report)
        return report

    def _log_report(self, report: ResilienceReport) -> None:
        status = "PASSED" if report.passed else "FAILED"
        logger.info(
            f"\n{'='*60}\n"
            f"RESILIENCE SCORE: {report.score}/100 → {status}\n"
            f"{'='*60}\n"
            f"  Benchmarks:         {report.benchmark_score:.1f}/30\n"
            f"  Chaos experiments:  {report.chaos_score:.1f}/50\n"
            f"  PONR stability:     {report.ponr_stability_score:.1f}/10\n"
            f"  Anomaly accuracy:   {report.anomaly_accuracy_score:.1f}/10\n"
            f"{'='*60}\n"
            f"Recommendation: {report.recommendation}\n"
            f"{'='*60}"
        )

    def save(self, report: ResilienceReport, output_dir: Path) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "resilience_report.json"

        def _default(obj):
            if hasattr(obj, "__dataclass_fields__"):
                return asdict(obj)
            if hasattr(obj, "value"):
                return obj.value
            return str(obj)

        data = {
            "score": report.score,
            "passed": report.passed,
            "evaluated_at": report.evaluated_at,
            "pass_threshold": self.PASS_THRESHOLD,
            "component_scores": {
                "benchmarks": report.benchmark_score,
                "chaos_experiments": report.chaos_score,
                "ponr_stability": report.ponr_stability_score,
                "anomaly_accuracy": report.anomaly_accuracy_score,
            },
            "benchmarks": [asdict(b) for b in report.benchmarks],
            "experiments": [
                {
                    "id": e.experiment_id,
                    "name": e.name,
                    "state": e.state.value,
                    "score_contribution": e.score_contribution,
                    "pass_criteria": e.pass_criteria,
                    "recovery_verified": e.recovery_verified,
                    "duration_seconds": e.duration_seconds,
                    "error": e.error,
                }
                for e in report.experiments
            ],
            "blocking_failures": report.blocking_failures,
            "recommendation": report.recommendation,
        }

        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=_default)

        logger.info(f"Resilience report saved → {path}")
        return path
