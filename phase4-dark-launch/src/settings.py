from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List


class Settings(BaseSettings):
    # Source DB (edge — still authoritative in Phase 4)
    source_host: str = Field("source", env="SOURCE_HOST")
    source_port: int = Field(3306, env="SOURCE_PORT")
    source_user: str = Field("migratex_reader", env="SOURCE_USER")
    source_password: str = Field(..., env="SOURCE_PASSWORD")
    source_database: str = Field(..., env="SOURCE_DATABASE")

    # Target DB (cloud — shadow in Phase 4)
    target_host: str = Field("target", env="TARGET_HOST")
    target_port: int = Field(3306, env="TARGET_PORT")
    target_user: str = Field("migratex_writer", env="TARGET_USER")
    target_password: str = Field(..., env="TARGET_PASSWORD")
    target_database: str = Field(..., env="TARGET_DATABASE")

    # Shadow write
    shadow_write_timeout_ms: int = Field(500, env="SHADOW_WRITE_TIMEOUT_MS")
    shadow_error_rate_alert_threshold: float = Field(0.01, env="SHADOW_ERROR_RATE_ALERT_THRESHOLD")

    # Semantic audit
    audit_observation_window_hours: int = Field(72, env="AUDIT_OBSERVATION_WINDOW_HOURS")
    audit_class_a_restart_clock: bool = Field(True, env="AUDIT_CLASS_A_RESTART_CLOCK")
    audit_sample_rate: float = Field(1.0, env="AUDIT_SAMPLE_RATE")  # 1.0 = 100%

    # Critical business logic paths
    critical_paths: str = Field(
        "bookings,users,inventory,reviews,pricing,search",
        env="CRITICAL_PATHS"
    )

    # PII redaction
    hmac_key_rotation_hours: int = Field(24, env="HMAC_KEY_ROTATION_HOURS")
    pii_surface_map_path: str = Field(
        "../phase1-discovery/artifacts/pii_surface_map.json",
        env="PII_SURFACE_MAP_PATH"
    )
    fk_graph_path: str = Field(
        "../phase1-discovery/artifacts/fk_dependency_graph.json",
        env="FK_GRAPH_PATH"
    )

    # FinOps
    egress_baseline_price_per_gb: float = Field(0.09, env="EGRESS_BASELINE_PRICE_PER_GB")
    egress_spike_multiplier: float = Field(1.5, env="EGRESS_SPIKE_MULTIPLIER")
    wal_buffer_max_age_hours: int = Field(2, env="WAL_BUFFER_MAX_AGE_HOURS")
    migration_deadline_hours: int = Field(240, env="MIGRATION_DEADLINE_HOURS")  # 10 weeks

    # Observability
    prometheus_port: int = Field(8000, env="PROMETHEUS_PORT")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    @property
    def critical_paths_list(self) -> List[str]:
        return [p.strip() for p in self.critical_paths.split(",")]

    class Config:
        env_file = "config/.env"
        env_file_encoding = "utf-8"
        case_sensitive = False


settings = Settings()
