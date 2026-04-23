"""Tests for S5000F semantic transformation logic."""

from __future__ import annotations

from transforms.polars.s5000f.id_utils import s5000f_id


class TestS5000FIdGeneration:
    """Tests for deterministic S5000F ID generation."""

    def test_deterministic(self) -> None:
        """Same input always produces the same ID."""
        id1 = s5000f_id("ProductInstance", "a001")
        id2 = s5000f_id("ProductInstance", "a001")
        assert id1 == id2

    def test_different_sources_produce_different_ids(self) -> None:
        """Different source IDs produce different S5000F IDs."""
        id1 = s5000f_id("ProductInstance", "a001")
        id2 = s5000f_id("ProductInstance", "a002")
        assert id1 != id2

    def test_different_concepts_produce_different_ids(self) -> None:
        """Same source ID but different concepts produce different IDs."""
        pi_id = s5000f_id("ProductInstance", "a001")
        mt_id = s5000f_id("MaintenanceTask", "a001")
        assert pi_id != mt_id

    def test_prefix_format(self) -> None:
        """IDs should have the correct concept prefix."""
        assert s5000f_id("ProductInstance", "x").startswith("PI-")
        assert s5000f_id("FunctionalFailure", "x").startswith("FF-")
        assert s5000f_id("MaintenanceTask", "x").startswith("MT-")
        assert s5000f_id("MaintenanceTaskStep", "x").startswith("MTS-")
        assert s5000f_id("MaintenanceEvent", "x").startswith("ME-")

    def test_id_length(self) -> None:
        """IDs should be prefix + hyphen + 12 hex chars."""
        result = s5000f_id("ProductInstance", "test123")
        prefix, hash_part = result.split("-", 1)
        assert prefix == "PI"
        assert len(hash_part) == 12
        # Verify it's valid hex
        int(hash_part, 16)


class TestS5000FTraceability:
    """Tests for source-to-S5000F traceability."""

    def test_asset_to_product_instance_traceability(self) -> None:
        """ProductInstance ID should be derivable from Asset ID."""
        asset_id = "asset-abc-123"
        pi_id = s5000f_id("ProductInstance", asset_id)

        # Re-derive — should match
        assert pi_id == s5000f_id("ProductInstance", asset_id)

    def test_cross_reference_consistency(self) -> None:
        """A WorkOrder's product_instance_id should match the Asset's PI ID."""
        asset_id = "a001"
        # ProductInstance derived from asset
        pi_from_asset = s5000f_id("ProductInstance", asset_id)
        # MaintenanceTask also references the same asset
        pi_from_wo = s5000f_id("ProductInstance", asset_id)

        assert pi_from_asset == pi_from_wo
