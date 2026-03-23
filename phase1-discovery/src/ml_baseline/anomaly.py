"""
ml_baseline/anomaly.py
──────────────────────
Isolation Forest anomaly detector for cloud target database heartbeat.

Phase 1 role: collect 14-day baseline signal data and train the model.
Phase 2+ role: score live incoming heartbeat signals.

The Isolation Forest is trained on "healthy" behaviour.
It detects correlated anomalies that individual metric thresholds miss.
Example: P99 latency of 50ms is normal in isolation.
         P99 latency of 50ms + WAL lag of 8s + conn pool at 85% = anomaly.
         The forest sees the correlation. A threshold alert does not.

Training signal set (10-second resolution, 14-day window):
  - P99 SELECT latency per table (top 50 by read volume)
  - P99 INSERT/UPDATE/DELETE latency
  - Replication lag (seconds behind master)
  - WAL log write rate (bytes/second)
  - InnoDB buffer pool hit rate
  - Connection pool: active / idle / waiting
  - Deadlock frequency (per minute)
  - Long-running query count (> 1 second)
"""

from __future__ import annotations

import json
import logging
import pickle
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)


# ── Signal schema ────────────────────────────────────────────────────────────

SIGNAL_FEATURES = [
    "p99_select_latency_ms",
    "p99_write_latency_ms",
    "replication_lag_seconds",
    "wal_write_rate_bytes_per_sec",
    "buffer_pool_hit_rate_pct",
    "conn_active",
    "conn_idle",
    "conn_waiting",
    "deadlock_count_per_min",
    "long_query_count",
]


@dataclass
class HeartbeatSignal:
    """One 10-second observation window."""
    timestamp: str
    p99_select_latency_ms: float
    p99_write_latency_ms: float
    replication_lag_seconds: float
    wal_write_rate_bytes_per_sec: float
    buffer_pool_hit_rate_pct: float
    conn_active: int
    conn_idle: int
    conn_waiting: int
    deadlock_count_per_min: float
    long_query_count: int

    def to_feature_vector(self) -> list[float]:
        return [
            self.p99_select_latency_ms,
            self.p99_write_latency_ms,
            self.replication_lag_seconds,
            self.wal_write_rate_bytes_per_sec,
            self.buffer_pool_hit_rate_pct,
            float(self.conn_active),
            float(self.conn_idle),
            float(self.conn_waiting),
            self.deadlock_count_per_min,
            float(self.long_query_count),
        ]


@dataclass
class AnomalyScore:
    timestamp: str
    score: float                # 0.0 (normal) → 1.0 (anomaly)
    is_anomaly: bool
    threshold: float
    signal: HeartbeatSignal
    recommendation: str


@dataclass
class ModelMetadata:
    trained_at: str
    training_samples: int
    contamination: float
    alert_threshold: float
    feature_names: list[str]
    scaler_mean: list[float]
    scaler_std: list[float]
    calibration_results: list[dict]


# ── Detector ─────────────────────────────────────────────────────────────────


class AnomalyDetector:
    """
    Two-phase component:

    Phase 1 (training):   fit(signals) — trains on 14-day baseline
    Phase 2+ (scoring):   score(signal) — scores live heartbeat

    The model and scaler are persisted to disk and loaded in Phase 2.
    """

    def __init__(
        self,
        contamination: float = 0.05,
        alert_threshold: float = 0.7,
        n_estimators: int = 100,
        random_state: int = 42,
    ):
        self._contamination = contamination
        self._alert_threshold = alert_threshold
        self._forest = IsolationForest(
            n_estimators=n_estimators,
            contamination=contamination,
            random_state=random_state,
            n_jobs=-1,
        )
        self._scaler = StandardScaler()
        self._trained = False
        self._metadata: ModelMetadata | None = None

    def fit(self, signals: list[HeartbeatSignal]) -> ModelMetadata:
        """
        Train on the 14-day baseline dataset.
        Requires minimum 2,016 samples (14 days × 144 samples/day).
        """
        if len(signals) < 2016:
            logger.warning(
                f"Training set has only {len(signals)} samples. "
                f"Minimum recommended: 2,016 (14 days × 10s intervals). "
                f"Model accuracy may be reduced."
            )

        X_raw = np.array([s.to_feature_vector() for s in signals])
        X = self._scaler.fit_transform(X_raw)
        self._forest.fit(X)
        self._trained = True

        # Calibrate: determine alert threshold from training distribution
        raw_scores = self._forest.decision_function(X)
        # Isolation Forest: negative scores = more anomalous
        # Normalise to 0→1 range where 1 = most anomalous
        normalised = self._normalise_scores(raw_scores)

        calibration = self._run_calibration(normalised)

        self._metadata = ModelMetadata(
            trained_at=datetime.utcnow().isoformat(),
            training_samples=len(signals),
            contamination=self._contamination,
            alert_threshold=self._alert_threshold,
            feature_names=SIGNAL_FEATURES,
            scaler_mean=self._scaler.mean_.tolist(),
            scaler_std=self._scaler.scale_.tolist(),
            calibration_results=calibration,
        )

        logger.info(
            f"IsolationForest trained — "
            f"samples={len(signals)} "
            f"threshold={self._alert_threshold} "
            f"estimators={self._forest.n_estimators}"
        )
        return self._metadata

    def score(self, signal: HeartbeatSignal) -> AnomalyScore:
        """
        Score a single live heartbeat signal.
        Returns normalised anomaly score 0.0–1.0.
        """
        if not self._trained:
            raise RuntimeError("Model not trained. Call fit() first.")

        X_raw = np.array([signal.to_feature_vector()])
        X = self._scaler.transform(X_raw)
        raw_score = self._forest.decision_function(X)[0]
        normalised = float(self._normalise_scores(np.array([raw_score]))[0])

        is_anomaly = normalised >= self._alert_threshold

        if is_anomaly:
            rec = "AUTONOMOUS_PAUSE"
        elif normalised >= self._alert_threshold * 0.8:
            rec = "MONITOR_CLOSELY"
        else:
            rec = "NORMAL"

        result = AnomalyScore(
            timestamp=signal.timestamp,
            score=round(normalised, 4),
            is_anomaly=is_anomaly,
            threshold=self._alert_threshold,
            signal=signal,
            recommendation=rec,
        )

        if is_anomaly:
            logger.warning(
                f"ANOMALY DETECTED — score={normalised:.4f} "
                f"threshold={self._alert_threshold} "
                f"rec={rec}"
            )

        return result

    def score_batch(self, signals: list[HeartbeatSignal]) -> list[AnomalyScore]:
        return [self.score(s) for s in signals]

    def _normalise_scores(self, raw_scores: np.ndarray) -> np.ndarray:
        """
        Isolation Forest decision_function returns:
          - Positive values → more normal (further from decision boundary)
          - Negative values → more anomalous

        We flip and normalise to 0→1 where 1 = most anomalous.
        """
        flipped = -raw_scores
        min_v, max_v = flipped.min(), flipped.max()
        if max_v == min_v:
            return np.zeros_like(flipped)
        return (flipped - min_v) / (max_v - min_v)

    def _run_calibration(self, training_scores: np.ndarray) -> list[dict]:
        """
        Validate that the alert threshold correctly identifies
        the contamination fraction of training samples as anomalous.
        """
        flagged = np.sum(training_scores >= self._alert_threshold)
        expected = int(len(training_scores) * self._contamination)
        return [
            {
                "check": "contamination_match",
                "expected_anomalous": expected,
                "actual_flagged": int(flagged),
                "pct_difference": round(
                    abs(flagged - expected) / max(expected, 1) * 100, 2
                ),
                "passed": abs(flagged - expected) / max(expected, 1) < 0.20,
            }
        ]

    def save(self, output_dir: Path) -> Path:
        """Persist model and metadata to disk."""
        output_dir.mkdir(parents=True, exist_ok=True)
        model_path = output_dir / "isolation_forest.pkl"
        meta_path = output_dir / "model_metadata.json"

        with open(model_path, "wb") as f:
            pickle.dump({"forest": self._forest, "scaler": self._scaler}, f)

        if self._metadata:
            with open(meta_path, "w") as f:
                json.dump(asdict(self._metadata), f, indent=2)

        logger.info(f"Model saved → {output_dir}")
        return model_path

    @classmethod
    def load(cls, model_dir: Path, alert_threshold: float = 0.7) -> "AnomalyDetector":
        """Load a previously trained model from disk."""
        model_path = model_dir / "isolation_forest.pkl"
        if not model_path.exists():
            raise FileNotFoundError(f"No model at {model_path}")

        with open(model_path, "rb") as f:
            payload = pickle.load(f)

        detector = cls(alert_threshold=alert_threshold)
        detector._forest = payload["forest"]
        detector._scaler = payload["scaler"]
        detector._trained = True

        meta_path = model_dir / "model_metadata.json"
        if meta_path.exists():
            with open(meta_path) as f:
                meta_dict = json.load(f)
            detector._metadata = ModelMetadata(**{
                k: v for k, v in meta_dict.items()
                if k in ModelMetadata.__dataclass_fields__
            })

        logger.info(f"Model loaded from {model_dir}")
        return detector


# ── Synthetic baseline generator (for testing) ───────────────────────────────

def generate_synthetic_baseline(
    n_samples: int = 2016,
    seed: int = 42,
    inject_anomalies: bool = False,
    anomaly_fraction: float = 0.05,
) -> list[HeartbeatSignal]:
    """
    Generates a synthetic 14-day baseline for unit testing.
    Mimics realistic database heartbeat patterns with:
      - Diurnal load variation (higher during business hours)
      - Gradual weekly pattern
      - Realistic inter-metric correlations

    Used in tests and dry-run validation — never in production.
    """
    rng = np.random.default_rng(seed)
    signals = []

    for i in range(n_samples):
        # Simulate diurnal pattern
        hour = (i * 10 / 3600) % 24
        load_factor = 0.4 + 0.6 * max(0, math.sin((hour - 6) * math.pi / 12))

        # Inject anomalies in last fraction if requested
        is_injected = inject_anomalies and (i >= int(n_samples * (1 - anomaly_fraction)))

        if is_injected:
            # Anomalous state: high latency + high lag + pool pressure
            p99_select = float(rng.normal(500, 50))   # 500ms — very bad
            p99_write = float(rng.normal(300, 30))
            repl_lag = float(rng.normal(30, 5))        # 30s lag — critical
            conn_active = int(rng.normal(120, 10))
            bp_hit = float(rng.normal(85, 5))
        else:
            p99_select = float(rng.normal(20 + 30 * load_factor, 5))
            p99_write = float(rng.normal(15 + 20 * load_factor, 3))
            repl_lag = float(rng.normal(0.5 + load_factor, 0.2))
            conn_active = int(rng.normal(30 + 40 * load_factor, 8))
            bp_hit = float(rng.normal(99 - load_factor * 2, 0.5))

        signals.append(HeartbeatSignal(
            timestamp=datetime.utcnow().isoformat(),
            p99_select_latency_ms=max(p99_select, 1.0),
            p99_write_latency_ms=max(p99_write, 1.0),
            replication_lag_seconds=max(repl_lag, 0.0),
            wal_write_rate_bytes_per_sec=float(rng.normal(50_000_000 * load_factor, 5_000_000)),
            buffer_pool_hit_rate_pct=min(max(bp_hit, 50.0), 100.0),
            conn_active=max(conn_active, 0),
            conn_idle=int(rng.normal(50, 10)),
            conn_waiting=int(rng.normal(1 * load_factor, 1)),
            deadlock_count_per_min=float(max(rng.normal(0.1 * load_factor, 0.05), 0)),
            long_query_count=int(max(rng.normal(2 * load_factor, 1), 0)),
        ))

    return signals


import math  # needed for sin in generate_synthetic_baseline
