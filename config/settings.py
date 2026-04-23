"""Centralised configuration for the EAM Data Pipe PoC.

All settings are loaded from environment variables with sensible defaults.
No hard-coded paths — every value is parameterised.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings


class KafkaSettings(BaseSettings):
    """Kafka broker and topic configuration."""

    bootstrap_servers: str = "localhost:9092"
    topic_asset: str = "cdc.asset"
    topic_work_request: str = "cdc.work_request"
    topic_work_order: str = "cdc.work_order"
    topic_maintenance_action: str = "cdc.maintenance_action"

    model_config = {"env_prefix": "KAFKA_"}

    @property
    def topics(self) -> list[str]:
        """All CDC topic names."""
        return [
            self.topic_asset,
            self.topic_work_request,
            self.topic_work_order,
            self.topic_maintenance_action,
        ]

    def topic_for_entity(self, entity: str) -> str:
        """Return the Kafka topic name for a given entity type."""
        mapping: dict[str, str] = {
            "Asset": self.topic_asset,
            "WorkRequest": self.topic_work_request,
            "WorkOrder": self.topic_work_order,
            "MaintenanceAction": self.topic_maintenance_action,
        }
        topic = mapping.get(entity)
        if topic is None:
            raise ValueError(f"Unknown entity type: {entity}")
        return topic


class MinioSettings(BaseSettings):
    """MinIO (S3-compatible) object storage configuration."""

    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket: str = "warehouse"
    region: str = "us-east-1"

    model_config = {"env_prefix": "MINIO_"}

    @property
    def s3_endpoint(self) -> str:
        """Full HTTP endpoint for S3 API calls."""
        return f"http://{self.endpoint}"


class IcebergSettings(BaseSettings):
    """Iceberg REST catalog configuration."""

    catalog_uri: str = "http://localhost:8181"
    catalog_warehouse: str = "s3://warehouse"

    model_config = {"env_prefix": "ICEBERG_"}


class SimulatorSettings(BaseSettings):
    """EAM simulator defaults."""

    default_batch_size: int = 100
    random_seed: int = 42

    model_config = {"env_prefix": "SIMULATOR_"}


class Settings(BaseSettings):
    """Root settings — aggregates all sub-configurations."""

    kafka: KafkaSettings = KafkaSettings()
    minio: MinioSettings = MinioSettings()
    iceberg: IcebergSettings = IcebergSettings()
    simulator: SimulatorSettings = SimulatorSettings()

    # Source system identifier (constant for this PoC)
    source_system: str = "simulated-eam"
    source_version: str = "poc-v1"
