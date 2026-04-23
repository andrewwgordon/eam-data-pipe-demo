"""Tests for configuration module."""

from __future__ import annotations

import pytest

from config.settings import KafkaSettings, Settings


class TestKafkaSettings:
    """Tests for Kafka configuration."""

    def test_topics_list(self) -> None:
        """Should return all four CDC topics."""
        kafka = KafkaSettings()
        assert len(kafka.topics) == 4
        assert "cdc.asset" in kafka.topics
        assert "cdc.work_request" in kafka.topics

    def test_topic_for_entity(self) -> None:
        """Should map entity names to correct topics."""
        kafka = KafkaSettings()
        assert kafka.topic_for_entity("Asset") == "cdc.asset"
        assert kafka.topic_for_entity("WorkOrder") == "cdc.work_order"

    def test_unknown_entity_raises(self) -> None:
        """Should raise ValueError for unknown entity types."""
        kafka = KafkaSettings()
        with pytest.raises(ValueError, match="Unknown entity type"):
            kafka.topic_for_entity("UnknownEntity")


class TestSettings:
    """Tests for root settings."""

    def test_source_system_constant(self) -> None:
        """Source system should be the PoC constant."""
        settings = Settings()
        assert settings.source_system == "simulated-eam"
        assert settings.source_version == "poc-v1"
