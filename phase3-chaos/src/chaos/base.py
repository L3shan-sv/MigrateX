"""
chaos/base.py
─────────────
Base classes and experiment registry for the MigrateX chaos suite.

Design principles (from the fallback plan):
  1. Blast radius bounded to shadow traffic only
     — fault injection NEVER touches the production write path
  2. Every experiment is time-bounded
     — max fault duration: 10 minutes, auto-recovery at timeout
  3. Experiments are disabled if source primary shows anomaly score > 0.3
     — chaos never compounds a real production incident
  4. Every experiment has a defined hypothesis, pass criteria, and recovery check

Experiment lifecycle:
  PENDING → RUNNING → (PASSED | FAILED | ABORTED)

Each experiment:
  - Defines its blast radius
  - Injects the fault
  - Monitors recovery
  - Verifies pass criteria
  - Records result
  - Cleans up (even on failure)
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class ExperimentState(Enum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ABORTED = "aborted"
    SKIPPED = "skipped"


class BlastRadius(Enum):
    SHADOW_ONLY = "shadow_traffic_only"      # CDC consumer + network path only
    KAFKA_ONLY = "kafka_cluster"             # Kafka broker operations
    ETCD_ONLY = "etcd_cluster"              # etcd node operations
    SCHEMA_ONLY = "schema_migration"         # DDL on source via gh-ost
    NETWORK_PATH = "edge_cloud_network"      # Network path between edge and cloud


@dataclass
class ExperimentResult:
    experiment_id: str
    name: str
    hypothesis: str
    blast_radius: BlastRadius
    state: ExperimentState
    started_at: str
    completed_at: str
    duration_seconds: float
    pass_criteria: list[dict]        # each: {criterion, expected, actual, passed}
    fault_injected: str
    recovery_verified: bool
    score_contribution: float        # points contributed to resilience score
    notes: str = ""
    error: str | None = None


@dataclass
class ExperimentContext:
    """Runtime context passed to each experiment."""
    source_pool: Any       # DBPool for source
    target_pool: Any       # DBPool for target
    kafka_bootstrap: list[str]
    etcd_endpoints: list[str]
    kafka_connect_url: str
    sink_group_id: str
    anomaly_score_fn: Callable[[], float]   # returns current anomaly score
    merkle_verify_fn: Callable[[], float]   # returns current divergence %
    consumer_lag_fn: Callable[[], float]    # returns lag in seconds
    max_fault_duration_seconds: int = 600
    auto_recover: bool = True


class ChaosExperiment(ABC):
    """
    Abstract base class for all 7 chaos experiments.

    Subclasses implement:
      - hypothesis: str — what we expect to happen
      - blast_radius: BlastRadius — what is affected
      - inject_fault() — start the fault
      - verify_recovery() → bool — confirm system recovered correctly
      - check_pass_criteria() → list[dict] — evaluate all pass criteria
      - cleanup() — restore state (called even on failure)
    """

    # Maximum score contribution if this experiment passes
    SCORE_WEIGHT: float = 50.0 / 7  # 50 total points ÷ 7 experiments ≈ 7.14 each

    def __init__(self, experiment_id: str, name: str):
        self.experiment_id = experiment_id
        self.name = name
        self._state = ExperimentState.PENDING
        self._start_time: float | None = None
        self._ctx: ExperimentContext | None = None

    @property
    @abstractmethod
    def hypothesis(self) -> str:
        """What we expect to happen during and after fault injection."""

    @property
    @abstractmethod
    def blast_radius(self) -> BlastRadius:
        """What infrastructure is affected by this experiment."""

    @abstractmethod
    def inject_fault(self, ctx: ExperimentContext) -> str:
        """
        Inject the fault. Returns a description of what was injected.
        Must be time-bounded — caller enforces MAX_FAULT_DURATION.
        """

    @abstractmethod
    def verify_recovery(self, ctx: ExperimentContext) -> bool:
        """
        Verify the system recovered correctly from the fault.
        Called after fault duration expires or fault self-resolves.
        Returns True if recovery is confirmed.
        """

    @abstractmethod
    def check_pass_criteria(self, ctx: ExperimentContext) -> list[dict]:
        """
        Evaluate all pass criteria for this experiment.
        Returns list of {criterion, expected, actual, passed}.
        """

    def cleanup(self, ctx: ExperimentContext) -> None:
        """
        Restore any state modified during fault injection.
        Called even if the experiment fails or aborts.
        Subclasses override this if they modify state.
        """
        pass

    def run(self, ctx: ExperimentContext) -> ExperimentResult:
        """
        Execute the full experiment lifecycle.
        This method is the only public entry point.
        """
        self._ctx = ctx
        self._start_time = time.time()
        started_at = datetime.utcnow().isoformat()

        logger.info(
            f"\n{'='*60}\n"
            f"EXPERIMENT: {self.name}\n"
            f"Hypothesis: {self.hypothesis}\n"
            f"Blast radius: {self.blast_radius.value}\n"
            f"{'='*60}"
        )

        # Pre-flight: check production anomaly score
        anomaly_score = ctx.anomaly_score_fn()
        if anomaly_score > 0.3:
            logger.warning(
                f"Experiment '{self.name}' SKIPPED — "
                f"production anomaly score={anomaly_score:.3f} > 0.3. "
                f"Chaos experiments never compound a real incident."
            )
            return self._make_result(
                started_at=started_at,
                state=ExperimentState.SKIPPED,
                fault="none — pre-flight check failed",
                pass_criteria=[],
                recovery=False,
                notes=f"Skipped: anomaly_score={anomaly_score:.3f}",
            )

        self._state = ExperimentState.RUNNING
        fault_description = "none"
        recovery_verified = False
        pass_criteria = []
        error = None

        try:
            # Inject fault
            fault_description = self.inject_fault(ctx)
            logger.info(f"Fault injected: {fault_description}")

            # Wait for max fault duration (auto-recovery)
            self._wait_with_monitoring(ctx)

            # Verify recovery
            recovery_verified = self.verify_recovery(ctx)
            if not recovery_verified:
                logger.error(f"Recovery verification FAILED for '{self.name}'")

            # Check pass criteria
            pass_criteria = self.check_pass_criteria(ctx)
            all_passed = all(c["passed"] for c in pass_criteria)

            self._state = ExperimentState.PASSED if (
                recovery_verified and all_passed
            ) else ExperimentState.FAILED

        except Exception as e:
            error = str(e)
            self._state = ExperimentState.ABORTED
            logger.error(f"Experiment '{self.name}' ABORTED: {e}")

        finally:
            try:
                self.cleanup(ctx)
            except Exception as e:
                logger.error(f"Cleanup failed for '{self.name}': {e}")

        duration = time.time() - self._start_time
        completed_at = datetime.utcnow().isoformat()

        score = self.SCORE_WEIGHT if self._state == ExperimentState.PASSED else 0.0

        result = self._make_result(
            started_at=started_at,
            state=self._state,
            fault=fault_description,
            pass_criteria=pass_criteria,
            recovery=recovery_verified,
            duration=duration,
            score=score,
            error=error,
            completed_at=completed_at,
        )

        self._log_result(result)
        return result

    def _wait_with_monitoring(self, ctx: ExperimentContext) -> None:
        """
        Wait during fault injection while monitoring for production anomalies.
        If production anomaly score spikes > 0.3 during the experiment,
        trigger early recovery (never compound a real incident).
        """
        max_duration = ctx.max_fault_duration_seconds
        check_interval = 10  # seconds
        elapsed = 0

        while elapsed < max_duration:
            time.sleep(check_interval)
            elapsed += check_interval

            anomaly = ctx.anomaly_score_fn()
            if anomaly > 0.5:
                logger.warning(
                    f"Production anomaly score={anomaly:.3f} during experiment "
                    f"'{self.name}' — triggering early recovery"
                )
                break

    def _make_result(
        self,
        started_at: str,
        state: ExperimentState,
        fault: str,
        pass_criteria: list[dict],
        recovery: bool,
        duration: float = 0.0,
        score: float = 0.0,
        error: str | None = None,
        completed_at: str | None = None,
    ) -> ExperimentResult:
        return ExperimentResult(
            experiment_id=self.experiment_id,
            name=self.name,
            hypothesis=self.hypothesis,
            blast_radius=self.blast_radius,
            state=state,
            started_at=started_at,
            completed_at=completed_at or datetime.utcnow().isoformat(),
            duration_seconds=duration,
            pass_criteria=pass_criteria,
            fault_injected=fault,
            recovery_verified=recovery,
            score_contribution=score,
            error=error,
        )

    def _log_result(self, result: ExperimentResult) -> None:
        status = "PASSED" if result.state == ExperimentState.PASSED else "FAILED"
        logger.info(
            f"\n{'='*60}\n"
            f"RESULT: {result.name} → {status}\n"
            f"Duration: {result.duration_seconds:.1f}s\n"
            f"Recovery verified: {result.recovery_verified}\n"
            f"Score contribution: {result.score_contribution:.2f} pts\n"
            f"Pass criteria:\n" +
            "\n".join(
                f"  {'✓' if c['passed'] else '✗'} {c['criterion']}: "
                f"expected={c['expected']} actual={c['actual']}"
                for c in result.pass_criteria
            ) + f"\n{'='*60}"
        )
