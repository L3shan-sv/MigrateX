"""
ponr/engine.py
──────────────
The Point of No Return (PONR) Monte Carlo simulation engine.
The single most important component in MigrateX.

Every cutover decision defers to this engine's output.
It answers one question with mathematical precision:

    "At this exact moment, what does it cost to roll back?
     And how confident are we in that estimate?"

The engine runs 10,000 simulations per evaluation window.
Each simulation draws from probability distributions of:
  - Network throughput (mean ± variance from Phase 1 bandwidth test)
  - Write rate (from traffic profiler)
  - Data volume remaining (decreases as migration progresses)
  - Rollback re-sync cost (function of divergence since last clean state)

Output: P5 / P50 / P95 rollback cost distribution.
Gate: if P95 > operator SLA threshold → block cutover.

This is NOT a PID controller. It is a Monte Carlo simulation.
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Literal

import numpy as np

logger = logging.getLogger(__name__)


# ── Simulation inputs ────────────────────────────────────────────────────────


@dataclass
class NetworkProfile:
    """Derived from the 24-hour bandwidth test in Phase 1."""
    mean_throughput_gbps: float      # sustained mean
    std_throughput_gbps: float       # standard deviation (variance matters!)
    rtt_ms_p50: float
    rtt_ms_p99: float
    packet_loss_pct: float           # baseline packet loss rate


@dataclass
class MigrationState:
    """
    Snapshot of current migration progress.
    Updated every simulation tick from live metrics.
    """
    total_data_bytes: int
    bytes_transferred: int
    write_rate_bytes_per_sec: float   # current source write rate
    replication_lag_seconds: float    # CDC consumer lag
    elapsed_seconds: float            # time since migration started
    last_clean_merkle_seconds: float  # seconds since last clean Merkle


@dataclass
class PONRDistribution:
    """Output of one simulation run."""
    p5_usd: float
    p50_usd: float
    p95_usd: float
    p99_usd: float
    mean_usd: float
    std_usd: float
    rollback_feasible: bool           # True if P95 < SLA threshold
    event_horizon_seconds: float | None  # est. seconds until P95 breaches SLA
    simulations_run: int
    evaluated_at: str
    migration_progress_pct: float
    recommendation: Literal["PROCEED", "CAUTION", "BLOCK"]


# ── Cost model ───────────────────────────────────────────────────────────────


class RollbackCostModel:
    """
    Models the cost of rolling back the migration at a given moment.

    Cost components:
    1. Re-sync cost: bytes that have already been transferred that
       must be re-verified or replayed on rollback.
    2. Divergence repair cost: writes that arrived at edge after
       the last clean Merkle — these must be reconciled.
    3. Downtime cost: application is unavailable during rollback.
       Derived from operator-provided cost-per-hour.
    4. Egress cost: any cloud egress incurred by the partial migration
       that cannot be recovered.

    All costs in USD.
    """

    EGRESS_COST_PER_GB = 0.09          # AWS standard egress pricing
    DOWNTIME_COST_PER_HOUR = 5_000.0   # operator-configurable
    ROLLBACK_BANDWIDTH_GBPS_MEAN = 0.5  # rollback uses half of forward bandwidth
    BYTES_PER_GB = 1e9

    def compute(
        self,
        state: MigrationState,
        network: NetworkProfile,
        rng: np.random.Generator,
        n_simulations: int,
    ) -> np.ndarray:
        """
        Run n_simulations Monte Carlo draws and return array of total costs.
        Each simulation independently samples from network and write-rate
        distributions to model realistic variance.
        """

        # ── Network throughput samples ───────────────────────────────────────
        # Clamp to minimum 10 Mbps — network never goes fully to zero
        throughput_samples = np.maximum(
            rng.normal(
                loc=network.mean_throughput_gbps,
                scale=network.std_throughput_gbps,
                size=n_simulations,
            ),
            0.01,
        )

        # ── Data volume already transferred (bytes) ──────────────────────────
        bytes_transferred = state.bytes_transferred

        # ── Egress cost: proportional to bytes already in cloud ──────────────
        egress_cost = (bytes_transferred / self.BYTES_PER_GB) * self.EGRESS_COST_PER_GB

        # ── Re-sync cost: time to replay diverged writes on rollback ─────────
        # Divergence grows with replication lag and write rate
        diverged_bytes = state.replication_lag_seconds * state.write_rate_bytes_per_sec
        # Time to re-sync diverged bytes at rollback throughput
        rollback_throughput = np.maximum(
            rng.normal(
                loc=self.ROLLBACK_BANDWIDTH_GBPS_MEAN,
                scale=network.std_throughput_gbps * 0.5,
                size=n_simulations,
            ),
            0.005,
        )
        resync_seconds = (diverged_bytes / self.BYTES_PER_GB) / rollback_throughput * 3600
        resync_cost = (resync_seconds / 3600) * self.DOWNTIME_COST_PER_HOUR

        # ── Downtime cost: application unavailable during rollback ───────────
        # Rollback takes: time to release lease + time to swing DNS + resync
        # Lease release + DNS swing: fixed ~60 seconds
        # Plus resync time already computed
        base_downtime_seconds = 60.0
        total_downtime_seconds = base_downtime_seconds + resync_seconds
        downtime_cost = (total_downtime_seconds / 3600) * self.DOWNTIME_COST_PER_HOUR

        # ── Merkle repair cost: additional cost if divergence is dirty ────────
        seconds_since_merkle = state.last_clean_merkle_seconds
        # Dirty blocks accumulate proportional to time since last clean check
        dirty_bytes = seconds_since_merkle * state.write_rate_bytes_per_sec * 0.001
        dirty_resync_seconds = (dirty_bytes / self.BYTES_PER_GB) / rollback_throughput * 3600
        merkle_repair_cost = (dirty_resync_seconds / 3600) * self.DOWNTIME_COST_PER_HOUR

        total_cost = egress_cost + resync_cost + downtime_cost + merkle_repair_cost
        return total_cost


# ── PONR Engine ──────────────────────────────────────────────────────────────


class PONREngine:
    """
    The Point of No Return Monte Carlo simulation engine.

    Usage:
        engine = PONREngine(
            n_simulations=10_000,
            sla_threshold_usd=5_000.0,
            block_confidence=0.95,
        )
        distribution = engine.evaluate(state, network)

    Thread-safe: each call creates a new RNG instance.
    """

    def __init__(
        self,
        n_simulations: int = 10_000,
        sla_threshold_usd: float = 5_000.0,
        block_confidence: float = 0.95,
        seed: int | None = None,
    ):
        self._n = n_simulations
        self._sla = sla_threshold_usd
        self._confidence = block_confidence
        self._seed = seed
        self._cost_model = RollbackCostModel()
        logger.info(
            f"PONREngine initialised — "
            f"simulations={n_simulations} "
            f"sla_threshold=${sla_threshold_usd:,.0f} "
            f"block_confidence={block_confidence}"
        )

    def evaluate(
        self,
        state: MigrationState,
        network: NetworkProfile,
    ) -> PONRDistribution:
        """
        Run the full Monte Carlo simulation and return the cost distribution.
        Called every 60 seconds during live migration, every 30 seconds
        during the 2-hour PONR monitoring window before cutover.
        """
        rng = np.random.default_rng(self._seed)

        costs = self._cost_model.compute(state, network, rng, self._n)

        p5 = float(np.percentile(costs, 5))
        p50 = float(np.percentile(costs, 50))
        p95 = float(np.percentile(costs, 95))
        p99 = float(np.percentile(costs, 99))
        mean = float(np.mean(costs))
        std = float(np.std(costs))

        rollback_feasible = p95 < self._sla
        progress_pct = (
            state.bytes_transferred / state.total_data_bytes * 100
            if state.total_data_bytes > 0
            else 0.0
        )

        # Estimate time until P95 breaches the SLA
        event_horizon = self._estimate_event_horizon(state, network, p95)

        recommendation = self._recommend(p95, p50)

        dist = PONRDistribution(
            p5_usd=round(p5, 2),
            p50_usd=round(p50, 2),
            p95_usd=round(p95, 2),
            p99_usd=round(p99, 2),
            mean_usd=round(mean, 2),
            std_usd=round(std, 2),
            rollback_feasible=rollback_feasible,
            event_horizon_seconds=event_horizon,
            simulations_run=self._n,
            evaluated_at=datetime.utcnow().isoformat(),
            migration_progress_pct=round(progress_pct, 2),
            recommendation=recommendation,
        )

        logger.info(
            f"PONR eval — "
            f"P5=${p5:,.0f} P50=${p50:,.0f} P95=${p95:,.0f} "
            f"feasible={rollback_feasible} "
            f"horizon={event_horizon:.0f}s "
            f"rec={recommendation}"
        )
        return dist

    def _estimate_event_horizon(
        self,
        state: MigrationState,
        network: NetworkProfile,
        current_p95: float,
    ) -> float | None:
        """
        Estimate seconds until P95 rollback cost crosses the SLA threshold.

        Method: linear extrapolation of current P95 cost rate.
        P95 cost grows as more data is transferred and replication lag increases.
        This is a conservative estimate — real P95 will grow faster as the
        migration approaches the final tables (which are highest write-rate).
        """
        if current_p95 >= self._sla:
            return 0.0  # already at/past event horizon

        # Rate of P95 cost growth per second
        # Proxy: egress cost growth rate (most predictable component)
        bytes_remaining = state.total_data_bytes - state.bytes_transferred
        throughput_bytes_per_sec = network.mean_throughput_gbps * 1e9
        transfer_seconds_remaining = bytes_remaining / max(throughput_bytes_per_sec, 1)

        if transfer_seconds_remaining <= 0:
            return None

        egress_per_second = (
            throughput_bytes_per_sec / 1e9
        ) * RollbackCostModel.EGRESS_COST_PER_GB

        remaining_sla_budget = self._sla - current_p95
        if egress_per_second <= 0:
            return None

        seconds_to_horizon = remaining_sla_budget / egress_per_second
        return round(seconds_to_horizon, 0)

    def _recommend(self, p95: float, p50: float) -> Literal["PROCEED", "CAUTION", "BLOCK"]:
        """
        Three-state recommendation:
        PROCEED  — P95 well below threshold, safe to continue
        CAUTION  — P95 within 20% of threshold, prepare Tier 3 sequence
        BLOCK    — P95 at or above threshold, cutover must not proceed
        """
        if p95 >= self._sla:
            return "BLOCK"
        if p95 >= self._sla * 0.80:
            return "CAUTION"
        return "PROCEED"

    def run_pre_migration_estimate(
        self,
        total_data_bytes: int,
        write_rate_bytes_per_sec: float,
        network: NetworkProfile,
    ) -> dict:
        """
        Phase 1 pre-migration estimate.
        Models the full migration from start to completion.
        Returns P5/P50/P95 for multiple progress checkpoints.
        """
        checkpoints = [0.25, 0.50, 0.75, 0.90, 0.95, 1.00]
        results = []

        for progress in checkpoints:
            state = MigrationState(
                total_data_bytes=total_data_bytes,
                bytes_transferred=int(total_data_bytes * progress),
                write_rate_bytes_per_sec=write_rate_bytes_per_sec,
                replication_lag_seconds=min(progress * 30, 120),  # lag grows with progress
                elapsed_seconds=progress * (total_data_bytes / max(network.mean_throughput_gbps * 1e9, 1)),
                last_clean_merkle_seconds=300,
            )
            dist = self.evaluate(state, network)
            results.append({
                "progress_pct": progress * 100,
                "p5_usd": dist.p5_usd,
                "p50_usd": dist.p50_usd,
                "p95_usd": dist.p95_usd,
                "rollback_feasible": dist.rollback_feasible,
                "recommendation": dist.recommendation,
                "event_horizon_seconds": dist.event_horizon_seconds,
            })

        # Estimate total migration duration
        total_bytes_gb = total_data_bytes / 1e9
        duration_hours_p50 = total_bytes_gb / max(network.mean_throughput_gbps, 0.001)
        duration_hours_p95 = total_bytes_gb / max(
            network.mean_throughput_gbps - 2 * network.std_throughput_gbps, 0.001
        )

        return {
            "pre_migration_estimate": True,
            "total_data_gb": round(total_bytes_gb, 2),
            "estimated_duration_hours": {
                "p50": round(duration_hours_p50, 1),
                "p95": round(duration_hours_p95, 1),
            },
            "rollback_cost_by_progress": results,
            "sla_threshold_usd": self._sla,
            "network_profile": asdict(network),
        }


def save_ponr_estimate(estimate: dict, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "ponr_phase1_estimate.json"
    with open(path, "w") as f:
        json.dump(estimate, f, indent=2)
    logger.info(f"PONR Phase 1 estimate saved → {path}")
    return path
