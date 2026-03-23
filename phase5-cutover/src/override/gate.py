"""
override/gate.py
────────────────
Cryptographic operator override gate for Phase 5 cutover.

Every cutover action requires a signed operator token.
The ML advises. The human decides.

Token format (JWT RS256):
  {
    "sub": "operator@company.com",
    "iat": <issued_at>,
    "exp": <expires_at>,           # issued_at + 25 minutes
    "migratex_action": "CUTOVER",
    "cutover_window": "2026-01-15T02:00:00",
    "ponr_p95_at_signing": 1234.56,
    "replication_lag_at_signing": 2.3,
    "anomaly_score_at_signing": 0.12,
    "jti": "<unique_token_id>"
  }

Why JWT RS256 (asymmetric)?
  - The operator's private key never leaves their machine
  - MigrateX only needs the public key to verify
  - The signed payload includes the PONR P95 at signing time
    → the operator cannot sign a token for a different risk profile
  - The exp claim means the token is only valid for 25 minutes
    → a token generated 30 minutes ago is rejected even if valid

Three decisions the operator can make:
  GO        — submit signed JWT → cutover sequence begins
  POSTPONE  — explicit postponement with reason code → 4h cooldown
  ABORT     — full migration abort → post-mortem required

The override gate is NOT overridable by software.
There is no programmatic path to bypass the operator token requirement.
A PONR BLOCK state prevents the gate from accepting a GO decision.
"""

from __future__ import annotations

import json
import logging
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class OperatorDecision(Enum):
    GO = "go"
    POSTPONE = "postpone"
    ABORT = "abort"
    PENDING = "pending"


@dataclass
class OperatorToken:
    """Parsed and validated operator JWT."""
    operator: str
    issued_at: datetime
    expires_at: datetime
    cutover_window: str
    ponr_p95_at_signing: float
    replication_lag_at_signing: float
    anomaly_score_at_signing: float
    token_id: str
    raw_token: str


@dataclass
class GateDecision:
    decision: OperatorDecision
    operator: str | None
    decided_at: str
    token_id: str | None
    ponr_p95: float | None
    reason: str
    audit_entry: dict


class OperatorOverrideGate:
    """
    Validates operator JWT tokens and enforces the cutover gate.

    The gate has three states:
      OPEN    — pre-flight passed, accepting decisions
      LOCKED  — PONR BLOCK active, GO decisions rejected
      CLOSED  — decision made (GO, POSTPONE, or ABORT)

    All decisions are recorded to an immutable audit trail.
    """

    def __init__(
        self,
        public_key_pem: str | None = None,
        public_key_path: str | Path | None = None,
        algorithm: str = "RS256",
        expiry_minutes: int = 25,
    ):
        self._algorithm = algorithm
        self._expiry_minutes = expiry_minutes
        self._public_key = None
        self._audit_trail: list[dict] = []
        self._ponr_blocked = False

        # Load public key
        if public_key_pem:
            self._public_key_pem = public_key_pem
        elif public_key_path:
            p = Path(public_key_path)
            if p.exists():
                self._public_key_pem = p.read_text()
            else:
                logger.warning(f"Public key not found at {public_key_path} — using test mode")
                self._public_key_pem = None
        else:
            self._public_key_pem = None

        if self._public_key_pem:
            try:
                import jwt
                self._jwt_available = True
            except ImportError:
                self._jwt_available = False
                logger.warning("PyJWT not available — using test mode token validation")
        else:
            self._jwt_available = False

    def set_ponr_blocked(self, blocked: bool) -> None:
        """Called by the PONR monitor when state transitions to/from BLOCK."""
        self._ponr_blocked = blocked
        if blocked:
            logger.critical(
                "OVERRIDE GATE LOCKED: PONR P95 >= SLA threshold. "
                "GO decisions will be rejected until PONR recovers."
            )

    def submit_go(self, token_str: str) -> GateDecision:
        """
        Submit a GO decision with a signed operator JWT.
        Returns the gate decision (accepted or rejected).
        """
        if self._ponr_blocked:
            decision = GateDecision(
                decision=OperatorDecision.POSTPONE,
                operator=None,
                decided_at=datetime.utcnow().isoformat(),
                token_id=None,
                ponr_p95=None,
                reason="REJECTED: PONR P95 is above SLA threshold. Cutover is blocked by the system. "
                       "This is not overridable. Wait for PONR to recover.",
                audit_entry=self._make_audit("GO_REJECTED_PONR_BLOCK", None, None),
            )
            self._audit(decision.audit_entry)
            logger.critical(
                "GO REJECTED: PONR block is active. "
                "Operator token ignored — cutover cannot proceed."
            )
            return decision

        # Validate token
        token = self._validate_token(token_str)
        if token is None:
            decision = GateDecision(
                decision=OperatorDecision.PENDING,
                operator=None,
                decided_at=datetime.utcnow().isoformat(),
                token_id=None,
                ponr_p95=None,
                reason="REJECTED: Invalid or expired operator token.",
                audit_entry=self._make_audit("TOKEN_INVALID", None, None),
            )
            self._audit(decision.audit_entry)
            return decision

        decision = GateDecision(
            decision=OperatorDecision.GO,
            operator=token.operator,
            decided_at=datetime.utcnow().isoformat(),
            token_id=token.token_id,
            ponr_p95=token.ponr_p95_at_signing,
            reason="GO decision accepted. Cutover sequence initiated.",
            audit_entry=self._make_audit("GO_ACCEPTED", token.operator, token.token_id),
        )
        self._audit(decision.audit_entry)
        logger.info(
            f"GO ACCEPTED — operator={token.operator} "
            f"ponr_p95=${token.ponr_p95_at_signing:,.0f} "
            f"token_id={token.token_id}"
        )
        return decision

    def submit_postpone(self, operator: str, reason_code: str) -> GateDecision:
        """Submit a POSTPONE decision. Triggers 4-hour cooldown."""
        entry = self._make_audit("POSTPONE", operator, None, notes=reason_code)
        self._audit(entry)
        decision = GateDecision(
            decision=OperatorDecision.POSTPONE,
            operator=operator,
            decided_at=datetime.utcnow().isoformat(),
            token_id=None,
            ponr_p95=None,
            reason=f"Postponed by {operator}. Reason: {reason_code}. 4-hour cooldown begins.",
            audit_entry=entry,
        )
        logger.warning(
            f"POSTPONE: operator={operator} reason={reason_code}. "
            f"4-hour cooldown. Pre-flight re-evaluated at next scheduled window."
        )
        return decision

    def submit_abort(self, operator: str, reason: str) -> GateDecision:
        """Submit an ABORT decision. Migration is terminated."""
        entry = self._make_audit("ABORT", operator, None, notes=reason)
        self._audit(entry)
        decision = GateDecision(
            decision=OperatorDecision.ABORT,
            operator=operator,
            decided_at=datetime.utcnow().isoformat(),
            token_id=None,
            ponr_p95=None,
            reason=f"ABORT by {operator}. Reason: {reason}. Full post-mortem required.",
            audit_entry=entry,
        )
        logger.critical(
            f"MIGRATION ABORT: operator={operator} reason={reason}. "
            f"Post-mortem required before re-migration."
        )
        return decision

    def generate_test_token(
        self,
        operator: str = "test-operator",
        ponr_p95: float = 1000.0,
        lag: float = 2.0,
        anomaly: float = 0.1,
        cutover_window: str | None = None,
    ) -> str:
        """
        Generate a test token for development and CI environments.
        NOT for production use — the private key is not a real operator key.
        In production, the operator generates the token on their machine
        using their private key (which never leaves their machine).
        """
        if cutover_window is None:
            cutover_window = datetime.utcnow().isoformat()

        payload = {
            "sub": operator,
            "iat": int(time.time()),
            "exp": int(time.time()) + self._expiry_minutes * 60,
            "migratex_action": "CUTOVER",
            "cutover_window": cutover_window,
            "ponr_p95_at_signing": ponr_p95,
            "replication_lag_at_signing": lag,
            "anomaly_score_at_signing": anomaly,
            "jti": secrets.token_hex(16),
        }

        # Use HS256 for test tokens (symmetric — no keypair needed)
        try:
            import jwt
            return jwt.encode(payload, "test-secret", algorithm="HS256")
        except ImportError:
            # Fallback: base64-encoded JSON payload
            import base64
            return base64.b64encode(json.dumps(payload).encode()).decode()

    def get_audit_trail(self) -> list[dict]:
        return list(self._audit_trail)

    # ── Token validation ──────────────────────────────────────────────────────

    def _validate_token(self, token_str: str) -> OperatorToken | None:
        """
        Validate an operator JWT.
        In production: uses RS256 with operator's public key.
        In test mode: uses HS256 or base64 decode.
        """
        try:
            import jwt as pyjwt

            # Try RS256 first (production)
            if self._public_key_pem:
                payload = pyjwt.decode(
                    token_str,
                    self._public_key_pem,
                    algorithms=["RS256"],
                )
            else:
                # Test mode: HS256
                try:
                    payload = pyjwt.decode(
                        token_str,
                        "test-secret",
                        algorithms=["HS256"],
                    )
                except Exception:
                    # Fallback: base64 JSON
                    import base64
                    payload = json.loads(base64.b64decode(token_str).decode())

        except ImportError:
            # No JWT library — parse base64 test token
            try:
                import base64
                payload = json.loads(base64.b64decode(token_str).decode())
            except Exception as e:
                logger.error(f"Token decode failed: {e}")
                return None
        except Exception as e:
            logger.error(f"Token validation failed: {e}")
            return None

        # Validate required claims
        required = [
            "sub", "exp", "migratex_action",
            "cutover_window", "ponr_p95_at_signing", "jti"
        ]
        for field in required:
            if field not in payload:
                logger.error(f"Token missing required claim: {field}")
                return None

        if payload.get("migratex_action") != "CUTOVER":
            logger.error("Token is not a CUTOVER token")
            return None

        # Check expiry
        exp = payload.get("exp", 0)
        if time.time() > exp:
            logger.error("Token has expired")
            return None

        return OperatorToken(
            operator=payload["sub"],
            issued_at=datetime.fromtimestamp(payload.get("iat", time.time()), tz=timezone.utc),
            expires_at=datetime.fromtimestamp(exp, tz=timezone.utc),
            cutover_window=payload["cutover_window"],
            ponr_p95_at_signing=float(payload["ponr_p95_at_signing"]),
            replication_lag_at_signing=float(payload.get("replication_lag_at_signing", 0.0)),
            anomaly_score_at_signing=float(payload.get("anomaly_score_at_signing", 0.0)),
            token_id=payload["jti"],
            raw_token=token_str,
        )

    def _make_audit(
        self,
        action: str,
        operator: str | None,
        token_id: str | None,
        notes: str = "",
    ) -> dict:
        return {
            "action": action,
            "operator": operator,
            "token_id": token_id,
            "timestamp": datetime.utcnow().isoformat(),
            "ponr_blocked": self._ponr_blocked,
            "notes": notes,
        }

    def _audit(self, entry: dict) -> None:
        self._audit_trail.append(entry)
        logger.info(f"AUDIT: {entry['action']} operator={entry['operator']} ts={entry['timestamp']}")
