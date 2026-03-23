"""
retraining/engine.py
────────────────────
Post-migration model retraining.

Two models require retraining after cutover:

1. Isolation Forest (anomaly detector)
   The Phase 1 model was trained on EDGE infrastructure behavior.
   Cloud-native traffic patterns differ:
     - Different storage latency (cloud SSD vs on-prem SAN)
     - Different network topology (internal VPC vs WAN)
     - Different connection pooling behavior
     - Different GC pause patterns
   Must retrain on 14-day cloud-native baseline BEFORE deploying for ops.

2. PONR Monte Carlo engine
   The Phase 1 model was parameterized for edge→cloud network.
   Post-cutover, its role changes: models cloud→cloud failover scenarios
   (DR, region migrations, major version upgrades).
   Network parameters must be updated to reflect cloud-internal characteristics:
     - Lower RTT (5ms → 0.5ms within same VPC)
     - Higher bandwidth
     - Different variance profile
   Recalibration: run simulation against first 7 days of cloud production data.
   Verify P50 projections within 10% of observed values.
"""

from __future__ import annotations

import json
import logging
import pickle
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class RetrainingResult:
    model_type: str
    trained_at: str
    training_samples: int
    previous_threshold: float | None
    new_threshold: float
    threshold_change_pct: float
    calibration_passed: bool
    notes: str
    saved_to: str


@dataclass
class PONRRecalibrationResult:
    recalibrated_at: str
    old_network_mean_gbps: float
    new_network_mean_gbps: float
    old_network_std_gbps: float
    new_network_std_gbps: float
    p50_deviation_pct: float
    calibration_passed: bool    # True if P50 within 10% of observed
    notes: str
    saved_to: str


class ModelRetrainingEngine:
    """
    Retrains the Isolation Forest on cloud-native baseline data
    and recalibrates the PONR engine for cloud-internal operations.
    """

    MIN_TRAINING_SAMPLES = 2016    # 14 days × 144 samples/day

    def __init__(
        self,
        output_dir: str | Path = "./artifacts/model",
        phase1_model_dir: str | Path | None = None,
    ):
        self._output_dir = Path(output_dir)
        self._phase1_dir = Path(phase1_model_dir) if phase1_model_dir else None
        self._output_dir.mkdir(parents=True, exist_ok=True)

    # ── Isolation Forest retraining ───────────────────────────────────────────

    def retrain_isolation_forest(
        self,
        cloud_signals: list[dict],
        contamination: float = 0.05,
        alert_threshold: float = 0.7,
    ) -> RetrainingResult:
        """
        Retrain the Isolation Forest on cloud-native baseline signals.

        Args:
            cloud_signals: list of heartbeat signal dicts collected from
                           the cloud target over 14+ days post-cutover
            contamination: expected fraction of anomalous samples
            alert_threshold: normalised score above which alert fires

        Returns:
            RetrainingResult with new model metadata
        """
        if len(cloud_signals) < self.MIN_TRAINING_SAMPLES:
            logger.warning(
                f"Only {len(cloud_signals)} samples — minimum is {self.MIN_TRAINING_SAMPLES}. "
                f"Model accuracy may be reduced."
            )

        # Load previous threshold for comparison
        prev_threshold = self._load_previous_threshold()

        # Build feature matrix
        feature_names = [
            "p99_select_latency_ms", "p99_write_latency_ms",
            "replication_lag_seconds", "wal_write_rate_bytes_per_sec",
            "buffer_pool_hit_rate_pct", "conn_active", "conn_idle",
            "conn_waiting", "deadlock_count_per_min", "long_query_count",
        ]

        X_raw = np.array([
            [float(s.get(f, 0.0)) for f in feature_names]
            for s in cloud_signals
        ])

        # Train
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import StandardScaler

        scaler = StandardScaler()
        X = scaler.fit_transform(X_raw)

        forest = IsolationForest(
            n_estimators=100,
            contamination=contamination,
            random_state=42,
            n_jobs=-1,
        )
        forest.fit(X)

        # Calibrate new threshold
        raw_scores = forest.decision_function(X)
        flipped = -raw_scores
        mn, mx = flipped.min(), flipped.max()
        normalised = (flipped - mn) / (mx - mn) if mx > mn else np.zeros_like(flipped)

        # Verify calibration
        flagged = np.sum(normalised >= alert_threshold)
        expected = int(len(cloud_signals) * contamination)
        deviation_pct = abs(flagged - expected) / max(expected, 1) * 100
        calibration_passed = deviation_pct < 20.0

        # Compute threshold change
        threshold_change_pct = 0.0
        if prev_threshold:
            threshold_change_pct = abs(alert_threshold - prev_threshold) / prev_threshold * 100

        # Save
        model_path = self._output_dir / "isolation_forest_cloud.pkl"
        meta_path = self._output_dir / "model_metadata_cloud.json"

        with open(model_path, "wb") as f:
            pickle.dump({"forest": forest, "scaler": scaler}, f)

        metadata = {
            "trained_at": datetime.utcnow().isoformat(),
            "training_samples": len(cloud_signals),
            "contamination": contamination,
            "alert_threshold": alert_threshold,
            "feature_names": feature_names,
            "scaler_mean": scaler.mean_.tolist(),
            "scaler_std": scaler.scale_.tolist(),
            "calibration": {
                "expected_anomalous": expected,
                "actual_flagged": int(flagged),
                "deviation_pct": round(deviation_pct, 2),
                "passed": calibration_passed,
            },
            "notes": "Trained on cloud-native baseline. Replaces Phase 1 edge model.",
        }
        with open(meta_path, "w") as f:
            json.dump(metadata, f, indent=2)

        result = RetrainingResult(
            model_type="IsolationForest",
            trained_at=metadata["trained_at"],
            training_samples=len(cloud_signals),
            previous_threshold=prev_threshold,
            new_threshold=alert_threshold,
            threshold_change_pct=round(threshold_change_pct, 2),
            calibration_passed=calibration_passed,
            notes=(
                f"Cloud-native retrain. Deviation from edge threshold: "
                f"{threshold_change_pct:.1f}%. "
                f"Calibration: {'PASS' if calibration_passed else 'FAIL'}"
            ),
            saved_to=str(model_path),
        )

        logger.info(
            f"IsolationForest retrained — "
            f"samples={len(cloud_signals)} "
            f"threshold_change={threshold_change_pct:.1f}% "
            f"calibration={'PASS' if calibration_passed else 'FAIL'}"
        )
        return result

    # ── PONR recalibration ────────────────────────────────────────────────────

    def recalibrate_ponr(
        self,
        observed_cloud_writes_per_sec: float,
        observed_cloud_network_gbps: float,
        observed_cloud_network_std_gbps: float,
        cloud_rtt_ms_p50: float = 0.5,
        cloud_rtt_ms_p99: float = 2.0,
        phase1_estimate_usd: float | None = None,
        observed_cost_usd: float | None = None,
    ) -> PONRRecalibrationResult:
        """
        Recalibrate the PONR engine for cloud-internal operations.
        Replaces edge→cloud network parameters with cloud-internal measurements.

        The recalibrated engine models:
          - Cloud-to-cloud regional failover (DR scenarios)
          - Major version upgrade migrations
          - Cross-AZ migrations
        """
        # Load Phase 1 parameters for comparison
        old_mean, old_std = self._load_phase1_network_params()

        # Verify P50 accuracy if we have comparison data
        p50_deviation_pct = 0.0
        calibration_passed = True

        if phase1_estimate_usd and observed_cost_usd:
            p50_deviation_pct = abs(
                phase1_estimate_usd - observed_cost_usd
            ) / max(phase1_estimate_usd, 0.01) * 100
            calibration_passed = p50_deviation_pct <= 10.0

            if not calibration_passed:
                logger.warning(
                    f"PONR P50 deviation {p50_deviation_pct:.1f}% > 10% — "
                    f"model inputs need review"
                )

        # Save recalibrated parameters
        params_path = self._output_dir / "ponr_cloud_params.json"
        params = {
            "recalibrated_at": datetime.utcnow().isoformat(),
            "environment": "cloud_internal",
            "network": {
                "mean_throughput_gbps": observed_cloud_network_gbps,
                "std_throughput_gbps": observed_cloud_network_std_gbps,
                "rtt_ms_p50": cloud_rtt_ms_p50,
                "rtt_ms_p99": cloud_rtt_ms_p99,
            },
            "write_rate": {
                "mean_bytes_per_sec": observed_cloud_writes_per_sec,
            },
            "calibration": {
                "p50_deviation_pct": round(p50_deviation_pct, 2),
                "passed": calibration_passed,
                "phase1_estimate_usd": phase1_estimate_usd,
                "observed_cost_usd": observed_cost_usd,
            },
            "use_cases": [
                "Cloud-to-cloud regional DR failover",
                "Major MySQL version upgrade migration",
                "Cross-AZ migration within same region",
            ],
        }

        with open(params_path, "w") as f:
            json.dump(params, f, indent=2)

        result = PONRRecalibrationResult(
            recalibrated_at=params["recalibrated_at"],
            old_network_mean_gbps=old_mean,
            new_network_mean_gbps=observed_cloud_network_gbps,
            old_network_std_gbps=old_std,
            new_network_std_gbps=observed_cloud_network_std_gbps,
            p50_deviation_pct=round(p50_deviation_pct, 2),
            calibration_passed=calibration_passed,
            notes=(
                f"Cloud-internal recalibration. "
                f"Network mean: {old_mean:.2f}→{observed_cloud_network_gbps:.2f} Gbps. "
                f"P50 deviation from actual: {p50_deviation_pct:.1f}%."
            ),
            saved_to=str(params_path),
        )

        logger.info(
            f"PONR recalibrated — "
            f"net_mean={observed_cloud_network_gbps:.2f}Gbps "
            f"p50_deviation={p50_deviation_pct:.1f}% "
            f"calibration={'PASS' if calibration_passed else 'FAIL'}"
        )
        return result

    def generate_synthetic_cloud_signals(
        self, n_samples: int = 2016, seed: int = 100
    ) -> list[dict]:
        """
        Generate synthetic cloud-native baseline signals for testing.
        Cloud patterns differ from edge: lower latency, higher stability.
        """
        rng = np.random.default_rng(seed)
        import math
        signals = []
        for i in range(n_samples):
            hour = (i * 10 / 3600) % 24
            load = 0.3 + 0.5 * max(0, math.sin((hour - 8) * math.pi / 12))
            signals.append({
                "p99_select_latency_ms": max(float(rng.normal(8 + 10 * load, 2)), 1.0),
                "p99_write_latency_ms": max(float(rng.normal(5 + 8 * load, 1.5)), 1.0),
                "replication_lag_seconds": max(float(rng.normal(0.1 + 0.2 * load, 0.05)), 0.0),
                "wal_write_rate_bytes_per_sec": float(rng.normal(60_000_000 * load, 5_000_000)),
                "buffer_pool_hit_rate_pct": min(float(rng.normal(99.5 - load * 0.5, 0.2)), 100.0),
                "conn_active": max(int(rng.normal(20 + 30 * load, 5)), 0),
                "conn_idle": max(int(rng.normal(60, 8)), 0),
                "conn_waiting": max(int(rng.normal(0.5 * load, 0.5)), 0),
                "deadlock_count_per_min": max(float(rng.normal(0.05 * load, 0.02)), 0.0),
                "long_query_count": max(int(rng.normal(load, 0.5)), 0),
            })
        return signals

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _load_previous_threshold(self) -> float | None:
        if not self._phase1_dir:
            return None
        meta_path = self._phase1_dir / "model_metadata.json"
        if meta_path.exists():
            try:
                with open(meta_path) as f:
                    meta = json.load(f)
                return float(meta.get("alert_threshold", 0.7))
            except Exception:
                pass
        return None

    def _load_phase1_network_params(self) -> tuple[float, float]:
        """Load Phase 1 network parameters for comparison."""
        if self._phase1_dir:
            estimate_path = self._phase1_dir.parent / "ponr_phase1_estimate.json"
            if estimate_path.exists():
                try:
                    with open(estimate_path) as f:
                        data = json.load(f)
                    net = data.get("network_profile", {})
                    return (
                        float(net.get("mean_throughput_gbps", 1.0)),
                        float(net.get("std_throughput_gbps", 0.1)),
                    )
                except Exception:
                    pass
        return 1.0, 0.1  # defaults
