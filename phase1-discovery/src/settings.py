from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
import os


class Settings(BaseSettings):
    # Source DB
    source_host: str = Field("localhost", env="SOURCE_HOST")
    source_port: int = Field(3306, env="SOURCE_PORT")
    source_user: str = Field("migratex_reader", env="SOURCE_USER")
    source_password: str = Field(..., env="SOURCE_PASSWORD")
    source_database: str = Field(..., env="SOURCE_DATABASE")

    # Target DB
    target_host: str = Field("localhost", env="TARGET_HOST")
    target_port: int = Field(3307, env="TARGET_PORT")
    target_user: str = Field("migratex_reader", env="TARGET_USER")
    target_password: str = Field(..., env="TARGET_PASSWORD")
    target_database: str = Field(..., env="TARGET_DATABASE")

    # Discovery
    scan_threads: int = Field(8, env="SCAN_THREADS")
    growth_rate_days: int = Field(90, env="GROWTH_RATE_DAYS")
    traffic_profile_days: int = Field(7, env="TRAFFIC_PROFILE_DAYS")
    baseline_collection_days: int = Field(14, env="BASELINE_COLLECTION_DAYS")
    snapshot_chunk_size: int = Field(500_000, env="SNAPSHOT_CHUNK_SIZE")

    # PONR
    ponr_simulations: int = Field(10_000, env="PONR_SIMULATIONS")
    ponr_sla_threshold_usd: float = Field(5000.0, env="PONR_SLA_THRESHOLD_USD")
    ponr_block_confidence: float = Field(0.95, env="PONR_BLOCK_CONFIDENCE")

    # Anomaly detection
    anomaly_contamination: float = Field(0.05, env="ANOMALY_CONTAMINATION")
    anomaly_alert_threshold: float = Field(0.7, env="ANOMALY_ALERT_THRESHOLD")

    # PII
    pii_entropy_threshold: float = Field(3.5, env="PII_ENTROPY_THRESHOLD")
    pii_sample_size: int = Field(1000, env="PII_SAMPLE_SIZE")

    # Output
    output_dir: str = Field("./artifacts", env="OUTPUT_DIR")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    class Config:
        env_file = "config/.env"
        env_file_encoding = "utf-8"
        case_sensitive = False


settings = Settings()
