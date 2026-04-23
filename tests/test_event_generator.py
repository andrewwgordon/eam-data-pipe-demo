"""Tests for the EAM event generator."""

from __future__ import annotations

from eam_simulator.event_generator import EAMSimulator


class TestEAMSimulator:
    """Tests for the EAM lifecycle simulator."""

    def test_deterministic_with_seed(self) -> None:
        """Same seed produces identical event sequences."""
        sim1 = EAMSimulator(seed=42)
        sim2 = EAMSimulator(seed=42)

        events1 = sim1.generate_batch(30)
        events2 = sim2.generate_batch(30)

        assert len(events1) == len(events2)
        for e1, e2 in zip(events1, events2):
            assert e1["entity"] == e2["entity"]
            assert e1["op"] == e2["op"]
            assert e1["pk"] == e2["pk"]

    def test_generates_all_entity_types(self) -> None:
        """A batch should contain events for all four entity types."""
        sim = EAMSimulator(seed=42)
        events = sim.generate_batch(100)

        entities = {e["entity"] for e in events}
        assert "Asset" in entities
        assert "WorkRequest" in entities
        assert "WorkOrder" in entities
        assert "MaintenanceAction" in entities

    def test_generates_create_and_update_ops(self) -> None:
        """A batch should contain both create and update operations."""
        sim = EAMSimulator(seed=42)
        events = sim.generate_batch(100)

        ops = {e["op"] for e in events}
        assert "c" in ops
        assert "u" in ops

    def test_cdc_envelope_structure(self) -> None:
        """Every event must follow the CDC envelope schema."""
        sim = EAMSimulator(seed=42)
        events = sim.generate_batch(50)

        for event in events:
            assert "entity" in event
            assert "op" in event
            assert event["op"] in ("c", "u", "d")
            assert "event_ts" in event
            assert "pk" in event
            assert "id" in event["pk"]
            assert "before" in event
            assert "after" in event
            assert "source" in event
            assert event["source"]["system"] == "simulated-eam"

    def test_lifecycle_sequencing(self) -> None:
        """Assets should be created before they can be degraded."""
        sim = EAMSimulator(seed=42)
        events = sim.generate_batch(50)

        # Track which assets have been created
        created_assets: set[str] = set()
        for event in events:
            if event["entity"] == "Asset":
                if event["op"] == "c":
                    created_assets.add(event["pk"]["id"])
                elif event["op"] == "u":
                    # Update should only happen to already-created assets
                    assert event["pk"]["id"] in created_assets

    def test_batch_size_respected(self) -> None:
        """Generated events should not exceed requested batch size."""
        sim = EAMSimulator(seed=42)
        events = sim.generate_batch(20)
        assert len(events) <= 20

    def test_work_order_links_to_work_request(self) -> None:
        """Work orders should reference valid work request IDs."""
        sim = EAMSimulator(seed=42)
        events = sim.generate_batch(100)

        wr_ids: set[str] = set()
        for event in events:
            if event["entity"] == "WorkRequest" and event["op"] == "c":
                wr_ids.add(event["pk"]["id"])
            elif event["entity"] == "WorkOrder" and event["op"] == "c":
                wo_wr_id = event["after"].get("work_request_id", "")
                assert wo_wr_id in wr_ids, f"WorkOrder references unknown WR: {wo_wr_id}"
