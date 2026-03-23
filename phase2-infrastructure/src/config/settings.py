from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List


class Settings(BaseSettings):
    # Source DB
    source_host: str = Field("source", env="SOURCE_HOST")
    source_port: int = Field(3306, env="SOURCE_PORT")
    source_user: str = Field("migratex_reader", env="SOURCE_USER")
    source_password: str = Field(..., env="SOURCE_PASSWORD")
    source_database: str = Field(..., env="SOURCE_DATABASE")
    source_server_id: int = Field(1, env="SOURCE_SERVER_ID")

    # Target DB
    target_host: str = Field("target", env="TARGET_HOST")
    target_port: int = Field(3306, env="TARGET_PORT")
    target_user: str = Field("migratex_writer", env="TARGET_USER")
    target_password: str = Field(..., env="TARGET_PASSWORD")
    target_database: str = Field(..., env="TARGET_DATABASE")

    # Kafka
    kafka_bootstrap_servers: str = Field("kafka:29092", env="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_prefix: str = Field("migratex", env="KAFKA_TOPIC_PREFIX")
    kafka_partitions: int = Field(12, env="KAFKA_PARTITIONS")
    kafka_replication_factor: int = Field(1, env="KAFKA_REPLICATION_FACTOR")
    kafka_retention_ms: int = Field(604_800_000, env="KAFKA_RETENTION_MS")  # 7 days

    # Debezium
    kafka_connect_url: str = Field("http://kafka-connect:8083", env="KAFKA_CONNECT_URL")
    debezium_connector_name: str = Field("migratex-source", env="DEBEZIUM_CONNECTOR_NAME")
    schema_registry_url: str = Field("http://schema-registry:8081", env="SCHEMA_REGISTRY_URL")

    # etcd
    etcd_endpoints: str = Field("etcd0:2379,etcd1:2379,etcd2:2379", env="ETCD_ENDPOINTS")
    etcd_lease_ttl_seconds: int = Field(10, env="ETCD_LEASE_TTL_SECONDS")
    etcd_lease_key: str = Field("/migratex/write_authority", env="ETCD_LEASE_KEY")
    fencing_token_key: str = Field("/migratex/fencing_epoch", env="FENCING_TOKEN_KEY")

    # Sink consumer
    sink_worker_threads: int = Field(8, env="SINK_WORKER_THREADS")
    sink_max_poll_records: int = Field(500, env="SINK_MAX_POLL_RECORDS")
    sink_commit_interval_ms: int = Field(5000, env="SINK_COMMIT_INTERVAL_MS")
    sink_dead_letter_topic: str = Field("migratex.dlq", env="SINK_DEAD_LETTER_TOPIC")
    sink_dedup_table: str = Field("_migratex_gtid_dedup", env="SINK_DEDUP_TABLE")
    sink_dedup_retention_days: int = Field(30, env="SINK_DEDUP_RETENTION_DAYS")

    # Snapshot
    snapshot_chunk_size: int = Field(500_000, env="SNAPSHOT_CHUNK_SIZE")
    snapshot_max_workers: int = Field(8, env="SNAPSHOT_MAX_WORKERS")
    snapshot_network_cap_pct: int = Field(70, env="SNAPSHOT_NETWORK_CAP_PCT")
    snapshot_deadman_hours: int = Field(4, env="SNAPSHOT_DEADMAN_HOURS")

    # Observability
    prometheus_port: int = Field(8000, env="PROMETHEUS_PORT")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    # Synthetic validation
    synthetic_volume_gb: float = Field(10.0, env="SYNTHETIC_VOLUME_GB")
    synthetic_replay_duration_seconds: int = Field(3600, env="SYNTHETIC_REPLAY_DURATION_SECONDS")

    @property
    def etcd_endpoints_list(self) -> List[str]:
        return [e.strip() for e in self.etcd_endpoints.split(",")]

    @property
    def kafka_bootstrap_list(self) -> List[str]:
        return [b.strip() for b in self.kafka_bootstrap_servers.split(",")]

    class Config:
        env_file = "config/.env"
        env_file_encoding = "utf-8"
        case_sensitive = False


settings = Settings()
