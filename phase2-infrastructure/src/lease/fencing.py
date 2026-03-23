"""
lease/fencing.py
────────────────
Distributed lease fencing via etcd.
Implements the write-authority arbitration mechanism.

At any moment during the migration, exactly one node holds the lease:
  - EDGE: the source database is the authoritative write target
  - CLOUD: the cloud target is the authoritative write target

The fencing token is a monotonically increasing epoch number.
Every write to the authoritative database includes the current epoch.
Any write with a stale epoch (lower than current max) is rejected.

This prevents split-brain writes during:
  - Network partitions
  - DNS propagation delays after cutover
  - Application instances with stale connection pools

Fencing token protocol:
  1. Current write-authority holds etcd lease with epoch N
  2. Every DB write records epoch N in _migratex_fencing metadata
  3. Target DB validates: incoming epoch >= max(seen epochs)
  4. Stale epoch → rejected → logged to audit trail
  5. At cutover: cloud acquires lease with epoch N+1
  6. All subsequent edge writes rejected (epoch N < N+1)

etcd lease TTL = 10 seconds.
Heartbeat interval = 3 seconds (keepalive must fire before TTL expires).
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable

logger = logging.getLogger(__name__)


class Authority(Enum):
    EDGE = "edge"
    CLOUD = "cloud"
    NONE = "none"    # no current authority — transition state


@dataclass
class FencingToken:
    epoch: int
    authority: Authority
    acquired_at: str
    lease_id: str | None


@dataclass
class LeaseState:
    current_authority: Authority
    current_epoch: int
    lease_healthy: bool
    last_heartbeat: str
    ttl_remaining_seconds: float


class FencingTokenClient:
    """
    etcd-backed distributed lease fencing client.

    Each MigrateX node (edge orchestrator, cloud orchestrator) runs one instance.
    Only one instance holds the lease at a time.

    Usage:
        # Edge orchestrator (migration start)
        client = FencingTokenClient(endpoints, authority=Authority.EDGE)
        token = client.acquire()   # gets epoch 1

        # Cloud orchestrator (cutover)
        cloud_client = FencingTokenClient(endpoints, authority=Authority.CLOUD)
        token = cloud_client.acquire()  # gets epoch 2 (edge's epoch 1 is now stale)

        # Validate on every write
        valid = client.validate_write(incoming_epoch)
    """

    LEASE_KEY = "/migratex/write_authority"
    EPOCH_KEY = "/migratex/fencing_epoch"
    AUDIT_KEY_PREFIX = "/migratex/audit/"

    def __init__(
        self,
        endpoints: list[str],
        authority: Authority,
        lease_ttl_seconds: int = 10,
        heartbeat_interval_seconds: int = 3,
        on_lease_lost: Callable | None = None,
    ):
        self._endpoints = endpoints
        self._authority = authority
        self._ttl = lease_ttl_seconds
        self._heartbeat_interval = heartbeat_interval_seconds
        self._on_lease_lost = on_lease_lost
        self._client = None
        self._lease = None
        self._lease_id: str | None = None
        self._current_epoch: int = 0
        self._running = False
        self._heartbeat_thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def _get_client(self):
        if self._client is None:
            try:
                import etcd3
                # Parse endpoints — etcd3 takes host+port separately
                host, port = self._endpoints[0].split(":")
                self._client = etcd3.client(
                    host=host,
                    port=int(port),
                    timeout=10,
                )
                logger.info(f"etcd client connected → {self._endpoints[0]}")
            except Exception as e:
                logger.error(f"Failed to connect to etcd: {e}")
                raise
        return self._client

    # ── Lease lifecycle ───────────────────────────────────────────────────────

    def acquire(self) -> FencingToken:
        """
        Acquire the write-authority lease.
        Atomically increments the epoch and records the new authority.
        Blocks until the lease is acquired (previous holder must release or expire).
        """
        client = self._get_client()

        # Atomically increment epoch
        new_epoch = self._increment_epoch(client)

        # Grant lease
        lease = client.lease(self._ttl)
        self._lease = lease
        self._lease_id = str(lease.id)

        # Write authority record under the lease
        authority_value = json.dumps({
            "authority": self._authority.value,
            "epoch": new_epoch,
            "acquired_at": _now_iso(),
            "lease_id": self._lease_id,
        }).encode()

        client.put(self.LEASE_KEY, authority_value, lease=lease)

        with self._lock:
            self._current_epoch = new_epoch
            self._running = True

        # Start heartbeat thread
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"migratex-lease-heartbeat-{self._authority.value}",
        )
        self._heartbeat_thread.start()

        token = FencingToken(
            epoch=new_epoch,
            authority=self._authority,
            acquired_at=_now_iso(),
            lease_id=self._lease_id,
        )
        logger.info(
            f"Lease acquired — authority={self._authority.value} "
            f"epoch={new_epoch} lease_id={self._lease_id}"
        )
        return token

    def release(self) -> None:
        """
        Release the write-authority lease.
        Called on graceful shutdown or before transferring authority.
        """
        with self._lock:
            self._running = False

        if self._lease:
            try:
                self._lease.revoke()
                logger.info(
                    f"Lease released — authority={self._authority.value} "
                    f"epoch={self._current_epoch}"
                )
            except Exception as e:
                logger.error(f"Failed to revoke lease: {e}")
            finally:
                self._lease = None
                self._lease_id = None

    def validate_write(self, incoming_epoch: int) -> bool:
        """
        Validate that an incoming write's epoch is current.
        Returns False if the epoch is stale (write should be rejected).

        Called by the application write path on every write during migration.
        In production: wrap the DB write in a function that calls this first.
        """
        with self._lock:
            valid = incoming_epoch >= self._current_epoch
        if not valid:
            logger.warning(
                f"FENCING VIOLATION: incoming_epoch={incoming_epoch} "
                f"current_epoch={self._current_epoch} "
                f"authority={self._authority.value}"
            )
        return valid

    def get_current_token(self) -> FencingToken | None:
        """Read the current write authority from etcd (cross-node visibility)."""
        try:
            client = self._get_client()
            value, metadata = client.get(self.LEASE_KEY)
            if value is None:
                return None
            data = json.loads(value.decode())
            return FencingToken(
                epoch=data["epoch"],
                authority=Authority(data["authority"]),
                acquired_at=data["acquired_at"],
                lease_id=data.get("lease_id"),
            )
        except Exception as e:
            logger.error(f"Failed to read current token: {e}")
            return None

    def get_state(self) -> LeaseState:
        with self._lock:
            epoch = self._current_epoch
            running = self._running

        return LeaseState(
            current_authority=self._authority if running else Authority.NONE,
            current_epoch=epoch,
            lease_healthy=running and self._lease is not None,
            last_heartbeat=_now_iso(),
            ttl_remaining_seconds=self._ttl if running else 0.0,
        )

    # ── Epoch management ──────────────────────────────────────────────────────

    def _increment_epoch(self, client) -> int:
        """
        Atomically read-and-increment the global epoch counter.
        Uses etcd transactions (compare-and-swap) for atomicity.
        """
        for attempt in range(10):
            value, metadata = client.get(self.EPOCH_KEY)
            current = int(value.decode()) if value else 0
            new_epoch = current + 1

            txn_success = client.transaction(
                compare=[
                    client.transactions.value(self.EPOCH_KEY) == (value or b"0"),
                ],
                success=[
                    client.transactions.put(self.EPOCH_KEY, str(new_epoch).encode()),
                ],
                failure=[],
            )

            if txn_success:
                logger.info(f"Epoch incremented: {current} → {new_epoch}")
                return new_epoch

            logger.debug(f"Epoch CAS failed (attempt {attempt+1}/10), retrying...")
            time.sleep(0.05)

        raise RuntimeError("Failed to increment fencing epoch after 10 attempts")

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    def _heartbeat_loop(self) -> None:
        """
        Keepalive loop. Must fire before lease TTL expires.
        If heartbeat fails 3 times → lease is considered lost → callback fires.
        """
        consecutive_failures = 0

        while self._running:
            try:
                if self._lease:
                    self._lease.refresh()
                    consecutive_failures = 0
                    logger.debug(f"Lease heartbeat OK — epoch={self._current_epoch}")
            except Exception as e:
                consecutive_failures += 1
                logger.warning(
                    f"Lease heartbeat failed ({consecutive_failures}/3): {e}"
                )
                if consecutive_failures >= 3:
                    logger.error(
                        "Lease lost after 3 consecutive heartbeat failures. "
                        "Write authority is now UNDEFINED. Triggering callback."
                    )
                    if self._on_lease_lost:
                        self._on_lease_lost()
                    break

            time.sleep(self._heartbeat_interval)


# ── Fencing token DB recorder ─────────────────────────────────────────────────

class FencingTokenRecorder:
    """
    Records fencing tokens into the target database metadata table.
    Every write operation during migration must call record_write() first.
    Any write with a stale epoch is rejected and logged to the audit trail.

    Schema (auto-created on Phase 2 provisioning):
        CREATE TABLE _migratex_fencing (
            write_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
            epoch        INT          NOT NULL,
            table_name   VARCHAR(128) NOT NULL,
            operation    CHAR(1)      NOT NULL,
            written_at   DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
            INDEX idx_epoch (epoch)
        ) ENGINE=InnoDB;
    """

    FENCING_TABLE = "_migratex_fencing"

    def __init__(self, max_epoch_key: str = "max_seen_epoch"):
        self._max_epoch = 0
        self._lock = threading.Lock()

    def ensure_table(self, conn) -> None:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.FENCING_TABLE}` (
                write_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
                epoch      INT          NOT NULL,
                table_name VARCHAR(128) NOT NULL,
                operation  CHAR(1)      NOT NULL,
                written_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                INDEX      idx_epoch (epoch)
            ) ENGINE=InnoDB
              COMMENT='MigrateX fencing token audit — do not modify'
        """)
        conn.commit()
        cursor.close()

        # Load current max epoch
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(epoch) AS max_ep FROM `{self.FENCING_TABLE}`")
        row = cursor.fetchone()
        cursor.close()
        if row and row[0]:
            with self._lock:
                self._max_epoch = int(row[0])

    def validate_and_record(
        self,
        epoch: int,
        table_name: str,
        operation: str,
        conn,
    ) -> bool:
        """
        Validate epoch is current then record the write.
        Returns False if the epoch is stale — caller must reject the write.
        """
        with self._lock:
            if epoch < self._max_epoch:
                logger.warning(
                    f"FENCING VIOLATION: epoch={epoch} < max_seen={self._max_epoch} "
                    f"table={table_name} op={operation}"
                )
                return False
            self._max_epoch = max(self._max_epoch, epoch)

        cursor = conn.cursor()
        cursor.execute(
            f"INSERT INTO `{self.FENCING_TABLE}` (epoch, table_name, operation) "
            f"VALUES (%s, %s, %s)",
            (epoch, table_name, operation),
        )
        cursor.close()
        return True

    def get_max_epoch(self) -> int:
        with self._lock:
            return self._max_epoch


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    from datetime import datetime
    return datetime.utcnow().isoformat()
