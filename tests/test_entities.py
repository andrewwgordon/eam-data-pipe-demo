"""Tests for EAM entity models and CDC payload generation."""

from __future__ import annotations

from eam_simulator.entities.asset import Asset, AssetStatus
from eam_simulator.entities.maintenance_action import MaintenanceAction, MaintenanceActionStatus
from eam_simulator.entities.work_order import WorkOrder, WorkOrderStatus
from eam_simulator.entities.work_request import (
    WorkRequest,
    WorkRequestPriority,
    WorkRequestStatus,
)


class TestAsset:
    """Tests for the Asset entity."""

    def test_create_cdc_payload(self) -> None:
        asset = Asset(
            id="a001",
            name="Pump-101",
            asset_type="pump",
            location="plant-A",
            status=AssetStatus.OPERATIONAL,
            install_date="2024-01-15",
            updated_at="2024-01-15T10:00:00+00:00",
        )
        payload = asset.to_cdc_payload(op="c")

        assert payload["entity"] == "Asset"
        assert payload["op"] == "c"
        assert payload["pk"] == {"id": "a001"}
        assert payload["after"]["name"] == "Pump-101"
        assert payload["after"]["status"] == "operational"
        assert payload["before"] == {}
        assert payload["source"]["system"] == "simulated-eam"

    def test_update_cdc_payload_includes_before(self) -> None:
        asset = Asset(id="a001", name="Pump-101", asset_type="pump", location="plant-A")
        before = asset._snapshot()
        asset.status = AssetStatus.DEGRADED
        payload = asset.to_cdc_payload(op="u", before=before)

        assert payload["op"] == "u"
        assert payload["before"]["status"] == "operational"
        assert payload["after"]["status"] == "degraded"

    def test_delete_cdc_payload_has_empty_after(self) -> None:
        asset = Asset(id="a001", name="Pump-101", asset_type="pump", location="plant-A")
        payload = asset.to_cdc_payload(op="d", before=asset._snapshot())

        assert payload["op"] == "d"
        assert payload["after"] == {}
        assert payload["before"]["id"] == "a001"


class TestWorkRequest:
    """Tests for the WorkRequest entity."""

    def test_create_payload(self) -> None:
        wr = WorkRequest(
            id="wr001",
            asset_id="a001",
            description="Pump leaking",
            priority=WorkRequestPriority.HIGH,
        )
        payload = wr.to_cdc_payload(op="c")

        assert payload["entity"] == "WorkRequest"
        assert payload["after"]["asset_id"] == "a001"
        assert payload["after"]["priority"] == "high"

    def test_status_transitions(self) -> None:
        wr = WorkRequest(id="wr001", asset_id="a001", description="Issue")
        assert wr.status == WorkRequestStatus.OPEN

        wr.status = WorkRequestStatus.APPROVED
        payload = wr.to_cdc_payload(op="u")
        assert payload["after"]["status"] == "approved"


class TestWorkOrder:
    """Tests for the WorkOrder entity."""

    def test_create_payload(self) -> None:
        wo = WorkOrder(id="wo001", work_request_id="wr001", asset_id="a001")
        payload = wo.to_cdc_payload(op="c")

        assert payload["entity"] == "WorkOrder"
        assert payload["after"]["status"] == "planned"
        assert payload["after"]["actual_start"] == ""

    def test_completion_sets_actual_end(self) -> None:
        wo = WorkOrder(
            id="wo001",
            work_request_id="wr001",
            asset_id="a001",
            status=WorkOrderStatus.COMPLETED,
            actual_start="2024-01-20T08:00:00+00:00",
            actual_end="2024-01-20T16:00:00+00:00",
        )
        payload = wo.to_cdc_payload(op="u")
        assert payload["after"]["actual_end"] != ""


class TestMaintenanceAction:
    """Tests for the MaintenanceAction entity."""

    def test_create_payload(self) -> None:
        action = MaintenanceAction(
            id="ma001",
            work_order_id="wo001",
            step_number=1,
            description="Inspect seals",
        )
        payload = action.to_cdc_payload(op="c")

        assert payload["entity"] == "MaintenanceAction"
        assert payload["after"]["step_number"] == 1
        assert payload["after"]["status"] == "pending"

    def test_completion_payload(self) -> None:
        action = MaintenanceAction(
            id="ma001",
            work_order_id="wo001",
            step_number=1,
            description="Inspect seals",
            status=MaintenanceActionStatus.COMPLETED,
            started_at="2024-01-20T09:00:00+00:00",
            completed_at="2024-01-20T10:00:00+00:00",
        )
        payload = action.to_cdc_payload(op="u")
        assert payload["after"]["status"] == "completed"
        assert payload["after"]["completed_at"] != ""
