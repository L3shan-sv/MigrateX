"""
scanner/schema.py
─────────────────
Programmatic scan of the source database schema.
Produces machine-readable artifacts consumed by:
  - The PONR engine (data volume + write rate)
  - The Merkle tree initialiser (table list + PK columns)
  - The PII surface mapper (column names + types)

All queries run against information_schema and performance_schema.
Zero writes. Zero locks. Safe on a live production read replica.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

from src.db import DBPool

logger = logging.getLogger(__name__)


# ── Data models ─────────────────────────────────────────────────────────────


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    column_type: str
    is_nullable: bool
    character_max_length: int | None
    column_key: str          # PRI, UNI, MUL, ""
    is_pii_candidate: bool = False
    pii_tier: int | None = None   # 1=direct, 2=quasi, 3=contextual


@dataclass
class IndexInfo:
    index_name: str
    columns: list[str]
    is_unique: bool
    index_type: str


@dataclass
class ForeignKey:
    constraint_name: str
    column: str
    referenced_table: str
    referenced_column: str


@dataclass
class TableInventory:
    schema_name: str
    table_name: str
    engine: str
    row_count_estimate: int
    data_size_bytes: int
    index_size_bytes: int
    avg_row_length: int
    columns: list[ColumnInfo] = field(default_factory=list)
    indexes: list[IndexInfo] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    primary_key_columns: list[str] = field(default_factory=list)
    has_triggers: bool = False
    has_stored_procs: bool = False


@dataclass
class SchemaInventory:
    database: str
    tables: list[TableInventory] = field(default_factory=list)
    total_data_bytes: int = 0
    total_index_bytes: int = 0
    total_row_count: int = 0
    fk_dependency_edges: list[dict] = field(default_factory=list)
    stored_procedures: list[str] = field(default_factory=list)
    triggers: list[str] = field(default_factory=list)
    views: list[str] = field(default_factory=list)


# ── PII heuristics ───────────────────────────────────────────────────────────

# Tier 1 — direct PII column name patterns
_PII_TIER1_PATTERNS = {
    "email", "e_mail", "email_address", "user_email",
    "phone", "phone_number", "mobile", "telephone",
    "first_name", "last_name", "full_name", "display_name",
    "ssn", "social_security", "passport", "national_id",
    "date_of_birth", "dob", "birth_date",
    "credit_card", "card_number", "cvv",
    "address", "street_address", "home_address",
    "ip_address", "user_ip",
}

# Tier 2 — quasi-PII
_PII_TIER2_PATTERNS = {
    "zip", "zipcode", "zip_code", "postal_code",
    "city", "country", "state", "region",
    "birth_year", "age",
    "latitude", "longitude", "lat", "lon", "lng",
    "device_id", "session_id", "cookie_id",
}

# Tier 3 — free-text that may contain PII
_PII_TIER3_TYPES = {"text", "mediumtext", "longtext", "tinytext", "json"}


def _classify_pii(col: ColumnInfo) -> tuple[bool, int | None]:
    name_lower = col.name.lower()
    for pattern in _PII_TIER1_PATTERNS:
        if pattern in name_lower:
            return True, 1
    for pattern in _PII_TIER2_PATTERNS:
        if pattern in name_lower:
            return True, 2
    if col.data_type.lower() in _PII_TIER3_TYPES:
        char_len = col.character_max_length or 0
        if char_len > 100 or char_len == 0:
            return True, 3
    return False, None


# ── Scanner ──────────────────────────────────────────────────────────────────


class SchemaScanner:
    """
    Scans source database schema via information_schema.
    All reads from read replica. Zero production impact.
    """

    def __init__(self, pool: DBPool, database: str):
        self._pool = pool
        self._database = database

    def run(self) -> SchemaInventory:
        logger.info(f"Starting schema scan → database={self._database}")
        inventory = SchemaInventory(database=self._database)

        tables = self._scan_tables()
        logger.info(f"Found {len(tables)} tables")

        for t in tables:
            t.columns = self._scan_columns(t.table_name)
            t.indexes = self._scan_indexes(t.table_name)
            t.foreign_keys = self._scan_foreign_keys(t.table_name)
            t.primary_key_columns = [
                c.name for c in t.columns if c.column_key == "PRI"
            ]
            t.has_triggers = self._has_triggers(t.table_name)
            inventory.tables.append(t)

        inventory.total_data_bytes = sum(t.data_size_bytes for t in inventory.tables)
        inventory.total_index_bytes = sum(t.index_size_bytes for t in inventory.tables)
        inventory.total_row_count = sum(t.row_count_estimate for t in inventory.tables)
        inventory.fk_dependency_edges = self._build_fk_graph(inventory.tables)
        inventory.stored_procedures = self._scan_stored_procedures()
        inventory.triggers = self._scan_triggers()
        inventory.views = self._scan_views()

        logger.info(
            f"Scan complete — tables={len(inventory.tables)} "
            f"data={inventory.total_data_bytes / 1e9:.2f}GB "
            f"rows={inventory.total_row_count:,}"
        )
        return inventory

    def _scan_tables(self) -> list[TableInventory]:
        rows = self._pool.execute(
            """
            SELECT
                TABLE_SCHEMA         AS schema_name,
                TABLE_NAME           AS table_name,
                ENGINE               AS engine,
                TABLE_ROWS           AS row_count_estimate,
                DATA_LENGTH          AS data_size_bytes,
                INDEX_LENGTH         AS index_size_bytes,
                AVG_ROW_LENGTH       AS avg_row_length
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
              AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY DATA_LENGTH DESC
            """,
            (self._database,),
        )
        return [
            TableInventory(
                schema_name=r["schema_name"],
                table_name=r["table_name"],
                engine=r["engine"] or "InnoDB",
                row_count_estimate=r["row_count_estimate"] or 0,
                data_size_bytes=r["data_size_bytes"] or 0,
                index_size_bytes=r["index_size_bytes"] or 0,
                avg_row_length=r["avg_row_length"] or 0,
            )
            for r in rows
        ]

    def _scan_columns(self, table_name: str) -> list[ColumnInfo]:
        rows = self._pool.execute(
            """
            SELECT
                COLUMN_NAME              AS name,
                DATA_TYPE                AS data_type,
                COLUMN_TYPE              AS column_type,
                IS_NULLABLE              AS is_nullable,
                CHARACTER_MAXIMUM_LENGTH AS character_max_length,
                COLUMN_KEY               AS column_key
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (self._database, table_name),
        )
        cols = []
        for r in rows:
            col = ColumnInfo(
                name=r["name"],
                data_type=r["data_type"],
                column_type=r["column_type"],
                is_nullable=r["is_nullable"] == "YES",
                character_max_length=r["character_max_length"],
                column_key=r["column_key"] or "",
            )
            col.is_pii_candidate, col.pii_tier = _classify_pii(col)
            cols.append(col)
        return cols

    def _scan_indexes(self, table_name: str) -> list[IndexInfo]:
        rows = self._pool.execute(
            """
            SELECT
                INDEX_NAME   AS index_name,
                COLUMN_NAME  AS column_name,
                NON_UNIQUE   AS non_unique,
                INDEX_TYPE   AS index_type
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """,
            (self._database, table_name),
        )
        index_map: dict[str, IndexInfo] = {}
        for r in rows:
            idx_name = r["index_name"]
            if idx_name not in index_map:
                index_map[idx_name] = IndexInfo(
                    index_name=idx_name,
                    columns=[],
                    is_unique=not bool(r["non_unique"]),
                    index_type=r["index_type"],
                )
            index_map[idx_name].columns.append(r["column_name"])
        return list(index_map.values())

    def _scan_foreign_keys(self, table_name: str) -> list[ForeignKey]:
        rows = self._pool.execute(
            """
            SELECT
                CONSTRAINT_NAME    AS constraint_name,
                COLUMN_NAME        AS column_name,
                REFERENCED_TABLE_NAME  AS ref_table,
                REFERENCED_COLUMN_NAME AS ref_column
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND REFERENCED_TABLE_NAME IS NOT NULL
            """,
            (self._database, table_name),
        )
        return [
            ForeignKey(
                constraint_name=r["constraint_name"],
                column=r["column_name"],
                referenced_table=r["ref_table"],
                referenced_column=r["ref_column"],
            )
            for r in rows
        ]

    def _has_triggers(self, table_name: str) -> bool:
        row = self._pool.execute_one(
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.TRIGGERS
            WHERE TRIGGER_SCHEMA = %s AND EVENT_OBJECT_TABLE = %s
            """,
            (self._database, table_name),
        )
        return (row["cnt"] if row else 0) > 0

    def _scan_stored_procedures(self) -> list[str]:
        rows = self._pool.execute(
            """
            SELECT ROUTINE_NAME AS name
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA = %s AND ROUTINE_TYPE = 'PROCEDURE'
            """,
            (self._database,),
        )
        return [r["name"] for r in rows]

    def _scan_triggers(self) -> list[str]:
        rows = self._pool.execute(
            """
            SELECT TRIGGER_NAME AS name
            FROM information_schema.TRIGGERS
            WHERE TRIGGER_SCHEMA = %s
            """,
            (self._database,),
        )
        return [r["name"] for r in rows]

    def _scan_views(self) -> list[str]:
        rows = self._pool.execute(
            """
            SELECT TABLE_NAME AS name
            FROM information_schema.VIEWS
            WHERE TABLE_SCHEMA = %s
            """,
            (self._database,),
        )
        return [r["name"] for r in rows]

    def _build_fk_graph(self, tables: list[TableInventory]) -> list[dict]:
        edges = []
        for t in tables:
            for fk in t.foreign_keys:
                edges.append({
                    "from_table": t.table_name,
                    "from_column": fk.column,
                    "to_table": fk.referenced_table,
                    "to_column": fk.referenced_column,
                    "constraint": fk.constraint_name,
                })
        return edges


# ── Artifact writer ──────────────────────────────────────────────────────────


def save_inventory(inventory: SchemaInventory, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "schema_inventory.json"

    def _serialise(obj: Any) -> Any:
        if hasattr(obj, "__dataclass_fields__"):
            return asdict(obj)
        raise TypeError(f"Not serialisable: {type(obj)}")

    with open(path, "w") as f:
        json.dump(asdict(inventory), f, indent=2, default=_serialise)

    fk_path = output_dir / "fk_dependency_graph.json"
    with open(fk_path, "w") as f:
        json.dump(inventory.fk_dependency_edges, f, indent=2)

    pii_map = {
        t.table_name: [
            {"column": c.name, "type": c.data_type, "pii_tier": c.pii_tier}
            for c in t.columns
            if c.is_pii_candidate
        ]
        for t in inventory.tables
        if any(c.is_pii_candidate for c in t.columns)
    }
    pii_path = output_dir / "pii_surface_map.json"
    with open(pii_path, "w") as f:
        json.dump(pii_map, f, indent=2)

    logger.info(f"Schema inventory saved → {output_dir}")
    return path
