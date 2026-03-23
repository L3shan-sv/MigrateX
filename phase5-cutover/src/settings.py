from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List


class Settings(BaseSettings):
    # Source DB (edge — authoritative until cutover)
    source_host: str = Field("source", env="SOURCE_HOST")
    source_port: int = Field(3306, env="SOURCE_PORT")
    source_user: str = Field("migratex_reader", env="SOURCE_USER")
    source_password: str = Field(..., env="SOURCE_PASSWORD")
    source_database: str = Field(..., env="SOURCE_DATABASE")

    # Target DB (cloud — becomes authoritative at cutover)
    target_host: str = Field("target", env="TARGET_HOST")
    target_port: int = Field(3306, env="TARGET_PORT")
    target_user: str = Field("migratex_writer", env="TARGET_USER")
    target_password: str = Field(..., env="TARGET_PASSWORD")
    target_database: str = Field(..., env="TARGET_DATABASE")

    # etcd
    etcd_endpoints: str = Field("etcd0:2379,etcd1:2379,etcd2:2379", env="ETCD_ENDPOINTS")
    etcd_lease_ttl_seconds: int = Field(10, env="ETCD_LEASE_TTL_SECONDS")
    etcd_lease_key: str = Field("/migratex/write_authority", env="ETCD_LEASE_KEY")
    fencing_epoch_key: str = Field("/migratex/fencing_epoch", env="FENCING_EPOCH_KEY")

    # PONR
    ponr_simulations: int = Field(10_000, env="PONR_SIMULATIONS")
    ponr_sla_threshold_usd: float = Field(5_000.0, env="PONR_SLA_THRESHOLD_USD")
    ponr_block_confidence: float = Field(0.95, env="PONR_BLOCK_CONFIDENCE")
    ponr_monitor_interval_seconds: int = Field(60, env="PONR_MONITOR_INTERVAL_SECONDS")
    ponr_window_interval_seconds: int = Field(30, env="PONR_WINDOW_INTERVAL_SECONDS")

    # Pre-flight
    preflight_replication_lag_max_seconds: float = Field(5.0, env="PREFLIGHT_REPLICATION_LAG_MAX")
    preflight_anomaly_score_max: float = Field(0.7, env="PREFLIGHT_ANOMALY_SCORE_MAX")
    preflight_merkle_max_age_minutes: int = Field(30, env="PREFLIGHT_MERKLE_MAX_AGE_MINUTES")
    preflight_conn_pool_idle_max_seconds: int = Field(30, env="PREFLIGHT_CONN_POOL_IDLE_MAX")
    preflight_check_interval_seconds: int = Field(60, env="PREFLIGHT_CHECK_INTERVAL_SECONDS")

    # Override gate
    jwt_algorithm: str = Field("RS256", env="JWT_ALGORITHM")
    jwt_expiry_minutes: int = Field(25, env="JWT_EXPIRY_MINUTES")
    operator_public_key_path: str = Field("./config/operator_public.pem", env="OPERATOR_PUBLIC_KEY_PATH")

    # Cutover sequence
    write_buffer_ms: int = Field(50, env="WRITE_BUFFER_MS")
    dns_ttl_seconds: int = Field(1, env="DNS_TTL_SECONDS")
    post_cutover_validation_seconds: int = Field(1800, env="POST_CUTOVER_VALIDATION_SECONDS")
    post_cutover_merkle_interval_minutes: int = Field(15, env="POST_CUTOVER_MERKLE_INTERVAL_MINUTES")
    connection_pool_max_idle_seconds: int = Field(30, env="CONNECTION_POOL_MAX_IDLE_SECONDS")

    # Final call
    final_call_window_minutes: int = Field(25, env="FINAL_CALL_WINDOW_MINUTES")
    final_call_notify_minutes_before: int = Field(30, env="FINAL_CALL_NOTIFY_MINUTES_BEFORE")

    # Observability
    prometheus_port: int = Field(8000, env="PROMETHEUS_PORT")
    pagerduty_integration_key: str = Field("", env="PAGERDUTY_INTEGRATION_KEY")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    @property
    def etcd_endpoints_list(self) -> List[str]:
        return [e.strip() for e in self.etcd_endpoints.split(",")]

    class Config:
        env_file = "config/.env"
        env_file_encoding = "utf-8"
        case_sensitive = False


settings = Settings()
