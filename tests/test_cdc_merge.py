"""Tests for CDC merge logic."""

from __future__ import annotations

import json

import polars as pl

from transforms.polars.app.cdc_merge import resolve_cdc


def _make_bronze_event(
    pk_id: str,
    op: str,
    entity_data: dict,
    event_ts: str = "2024-01-15T10:00:00+00:00",
) -> dict:
    """Helper to create a Bronze-format CDC row."""
    payload = {
        "entity": "Asset",
        "op": op,
        "event_ts": event_ts,
        "pk": {"id": pk_id},
        "before": {},
        "after": entity_data if op != "d" else {},
        "source": {"system": "simulated-eam", "version": "poc-v1"},
    }
    return {
        "pk_id": pk_id,
        "op": op,
        "event_ts": event_ts,
        "payload_json": json.dumps(payload),
        "source_system": "simulated-eam",
        "entity": "Asset",
        "event_date": event_ts[:10],
    }


ASSET_COLUMNS = ["id", "name", "asset_type", "location", "status", "install_date", "updated_at"]


class TestCDCMerge:
    """Tests for the CDC resolution logic."""

    def test_create_produces_record(self) -> None:
        """A single create event should produce one Silver record."""
        events = [
            _make_bronze_event("a001", "c", {
                "id": "a001", "name": "Pump-1", "asset_type": "pump",
                "location": "plant-A", "status": "operational",
                "install_date": "2024-01-15", "updated_at": "2024-01-15T10:00:00",
            }),
        ]
        bronze = pl.LazyFrame(events)
        result = resolve_cdc(bronze, None, ASSET_COLUMNS)

        assert len(result) == 1
        assert result["source_id"][0] == "a001"
        assert result["last_op"][0] == "c"

    def test_update_overwrites_create(self) -> None:
        """An update after a create should reflect the updated state."""
        events = [
            _make_bronze_event("a001", "c", {
                "id": "a001", "name": "Pump-1", "asset_type": "pump",
                "location": "plant-A", "status": "operational",
                "install_date": "2024-01-15", "updated_at": "2024-01-15T10:00:00",
            }, event_ts="2024-01-15T10:00:00+00:00"),
            _make_bronze_event("a001", "u", {
                "id": "a001", "name": "Pump-1", "asset_type": "pump",
                "location": "plant-A", "status": "degraded",
                "install_date": "2024-01-15", "updated_at": "2024-01-15T12:00:00",
            }, event_ts="2024-01-15T12:00:00+00:00"),
        ]
        bronze = pl.LazyFrame(events)
        result = resolve_cdc(bronze, None, ASSET_COLUMNS)

        assert len(result) == 1
        assert result["status"][0] == "degraded"
        assert result["last_op"][0] == "u"

    def test_delete_removes_record(self) -> None:
        """A delete event should remove the entity from Silver."""
        events = [
            _make_bronze_event("a001", "c", {
                "id": "a001", "name": "Pump-1", "asset_type": "pump",
                "location": "plant-A", "status": "operational",
                "install_date": "2024-01-15", "updated_at": "2024-01-15T10:00:00",
            }, event_ts="2024-01-15T10:00:00+00:00"),
            _make_bronze_event("a001", "d", {}, event_ts="2024-01-15T14:00:00+00:00"),
        ]
        bronze = pl.LazyFrame(events)
        result = resolve_cdc(bronze, None, ASSET_COLUMNS)

        assert len(result) == 0

    def test_idempotent_merge(self) -> None:
        """Running the same merge twice should produce identical results."""
        events = [
            _make_bronze_event("a001", "c", {
                "id": "a001", "name": "Pump-1", "asset_type": "pump",
                "location": "plant-A", "status": "operational",
                "install_date": "2024-01-15", "updated_at": "2024-01-15T10:00:00",
            }),
        ]
        bronze = pl.LazyFrame(events)

        result1 = resolve_cdc(bronze, None, ASSET_COLUMNS)
        # Run again with result1 as existing Silver
        result2 = resolve_cdc(bronze, result1.lazy(), ASSET_COLUMNS)

        assert len(result1) == len(result2)
        assert result1["source_id"].to_list() == result2["source_id"].to_list()

    def test_multiple_entities(self) -> None:
        """Multiple entities should each produce their own Silver record."""
        events = [
            _make_bronze_event("a001", "c", {
                "id": "a001", "name": "Pump-1", "asset_type": "pump",
                "location": "plant-A", "status": "operational",
                "install_date": "2024-01-15", "updated_at": "2024-01-15T10:00:00",
            }),
            _make_bronze_event("a002", "c", {
                "id": "a002", "name": "Motor-1", "asset_type": "motor",
                "location": "plant-B", "status": "operational",
                "install_date": "2024-01-16", "updated_at": "2024-01-16T10:00:00",
            }),
        ]
        bronze = pl.LazyFrame(events)
        result = resolve_cdc(bronze, None, ASSET_COLUMNS)

        assert len(result) == 2
        ids = set(result["source_id"].to_list())
        assert ids == {"a001", "a002"}
