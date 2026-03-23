import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager
from typing import Generator
import logging

logger = logging.getLogger(__name__)


class DBPool:
    """
    Thread-safe MySQL connection pool.
    Always connects to the READ REPLICA — never the primary.
    All Phase 1 operations are read-only.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        pool_size: int = 10,
        pool_name: str = "migratex_pool",
    ):
        self._config = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
            "autocommit": True,
            "connection_timeout": 30,
            "use_pure": True,
        }
        self._pool = pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            pool_reset_session=True,
            **self._config,
        )
        logger.info(f"DBPool initialised → {host}:{port}/{database} (pool={pool_size})")

    @contextmanager
    def cursor(self, dictionary: bool = True) -> Generator:
        conn = self._pool.get_connection()
        try:
            cur = conn.cursor(dictionary=dictionary)
            yield cur
            cur.close()
        except Exception as exc:
            logger.error(f"DB error: {exc}")
            raise
        finally:
            conn.close()

    def execute(self, query: str, params: tuple = ()) -> list[dict]:
        with self.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def execute_one(self, query: str, params: tuple = ()) -> dict | None:
        with self.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()

    def ping(self) -> bool:
        try:
            result = self.execute_one("SELECT 1 AS ok")
            return result is not None and result.get("ok") == 1
        except Exception:
            return False
