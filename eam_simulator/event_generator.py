"""EAM Simulator — generates realistic CDC event sequences.

Simulates the lifecycle of plant assets through degradation, work requests,
work orders, maintenance actions, and restoration. Maintains in-memory state
and emits CDC events for each state transition.

Lifecycle flow:
  Asset created → operational → degrades → work request raised →
  work request approved → work order planned → work order started →
  maintenance actions executed → work order completed → asset restored
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

from faker import Faker

from eam_simulator.entities.asset import Asset, AssetStatus
from eam_simulator.entities.maintenance_action import (
    MaintenanceAction,
    MaintenanceActionStatus,
)
from eam_simulator.entities.work_order import WorkOrder, WorkOrderStatus
from eam_simulator.entities.work_request import (
    WorkRequest,
    WorkRequestPriority,
    WorkRequestStatus,
)

# Asset types and locations for the simulated plant
ASSET_TYPES = ["pump", "compressor", "valve", "motor", "heat_exchanger", "conveyor"]
LOCATIONS = ["plant-A", "plant-B", "plant-C", "warehouse-1"]

# Maintenance action templates per asset type
ACTION_TEMPLATES: dict[str, list[str]] = {
    "pump": ["Inspect seals", "Replace impeller", "Lubricate bearings", "Test flow rate"],
    "compressor": ["Check pressure", "Replace filter", "Inspect valves", "Test output"],
    "valve": ["Inspect body", "Replace gasket", "Test actuation", "Calibrate"],
    "motor": ["Check windings", "Lubricate bearings", "Test insulation", "Align coupling"],
    "heat_exchanger": ["Inspect tubes", "Clean fouling", "Pressure test", "Check gaskets"],
    "conveyor": ["Inspect belt", "Tension adjustment", "Lubricate rollers", "Test alignment"],
}


def _now_iso() -> str:
    """Current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def _future_iso(hours: int = 24) -> str:
    """A timestamp `hours` in the future."""
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


class EAMSimulator:
    """In-memory EAM system that emits CDC events on state transitions.

    The simulator holds the current state of all entities and produces
    CDC event payloads whenever an entity is created, updated, or deleted.
    """

    def __init__(self, seed: int = 42) -> None:
        self.rng = random.Random(seed)
        self.fake = Faker()
        Faker.seed(seed)

        # Counter for deterministic ID generation
        self._id_counter = 0

        # In-memory entity stores (keyed by entity ID)
        self.assets: dict[str, Asset] = {}
        self.work_requests: dict[str, WorkRequest] = {}
        self.work_orders: dict[str, WorkOrder] = {}
        self.maintenance_actions: dict[str, MaintenanceAction] = {}

    def _new_id(self) -> str:
        """Generate a deterministic unique identifier using the seeded RNG."""
        self._id_counter += 1
        return f"{self.rng.getrandbits(48):012x}"

    # ── Asset lifecycle ──────────────────────────────────

    def create_asset(self) -> dict[str, Any]:
        """Create a new asset and return its CDC event."""
        asset_id = self._new_id()
        now = _now_iso()
        asset = Asset(
            id=asset_id,
            name=f"{self.fake.word().capitalize()}-{self.rng.randint(100, 999)}",
            asset_type=self.rng.choice(ASSET_TYPES),
            location=self.rng.choice(LOCATIONS),
            status=AssetStatus.OPERATIONAL,
            install_date=now[:10],  # date portion only
            updated_at=now,
        )
        self.assets[asset_id] = asset
        return asset.to_cdc_payload(op="c")

    def degrade_asset(self, asset_id: str) -> dict[str, Any] | None:
        """Transition an operational asset to degraded or failed."""
        asset = self.assets.get(asset_id)
        if asset is None or asset.status != AssetStatus.OPERATIONAL:
            return None

        before = asset._snapshot()
        new_status = self.rng.choice([AssetStatus.DEGRADED, AssetStatus.FAILED])
        asset.status = new_status
        asset.updated_at = _now_iso()
        return asset.to_cdc_payload(op="u", before=before)

    def restore_asset(self, asset_id: str) -> dict[str, Any] | None:
        """Restore a degraded/failed asset to operational."""
        asset = self.assets.get(asset_id)
        if asset is None or asset.status == AssetStatus.OPERATIONAL:
            return None

        before = asset._snapshot()
        asset.status = AssetStatus.OPERATIONAL
        asset.updated_at = _now_iso()
        return asset.to_cdc_payload(op="u", before=before)

    # ── Work request lifecycle ───────────────────────────

    def create_work_request(self, asset_id: str) -> dict[str, Any] | None:
        """Raise a work request against a degraded/failed asset."""
        asset = self.assets.get(asset_id)
        if asset is None or asset.status == AssetStatus.OPERATIONAL:
            return None

        wr_id = self._new_id()
        now = _now_iso()
        priority = (
            WorkRequestPriority.CRITICAL
            if asset.status == AssetStatus.FAILED
            else self.rng.choice(list(WorkRequestPriority))
        )
        wr = WorkRequest(
            id=wr_id,
            asset_id=asset_id,
            description=f"Reported issue on {asset.name}: {self.fake.sentence()}",
            priority=priority,
            status=WorkRequestStatus.OPEN,
            reported_at=now,
            updated_at=now,
        )
        self.work_requests[wr_id] = wr
        return wr.to_cdc_payload(op="c")

    def approve_work_request(self, wr_id: str) -> dict[str, Any] | None:
        """Approve an open work request."""
        wr = self.work_requests.get(wr_id)
        if wr is None or wr.status != WorkRequestStatus.OPEN:
            return None

        before = wr._snapshot()
        wr.status = WorkRequestStatus.APPROVED
        wr.updated_at = _now_iso()
        return wr.to_cdc_payload(op="u", before=before)

    def close_work_request(self, wr_id: str) -> dict[str, Any] | None:
        """Close an approved work request (after work order completes)."""
        wr = self.work_requests.get(wr_id)
        if wr is None or wr.status != WorkRequestStatus.APPROVED:
            return None

        before = wr._snapshot()
        wr.status = WorkRequestStatus.CLOSED
        wr.updated_at = _now_iso()
        return wr.to_cdc_payload(op="u", before=before)

    # ── Work order lifecycle ─────────────────────────────

    def create_work_order(self, wr_id: str) -> dict[str, Any] | None:
        """Create a planned work order from an approved work request."""
        wr = self.work_requests.get(wr_id)
        if wr is None or wr.status != WorkRequestStatus.APPROVED:
            return None

        wo_id = self._new_id()
        now = _now_iso()
        wo = WorkOrder(
            id=wo_id,
            work_request_id=wr_id,
            asset_id=wr.asset_id,
            status=WorkOrderStatus.PLANNED,
            planned_start=_future_iso(hours=self.rng.randint(1, 48)),
            planned_end=_future_iso(hours=self.rng.randint(49, 96)),
            updated_at=now,
        )
        self.work_orders[wo_id] = wo
        return wo.to_cdc_payload(op="c")

    def start_work_order(self, wo_id: str) -> dict[str, Any] | None:
        """Begin execution of a planned work order."""
        wo = self.work_orders.get(wo_id)
        if wo is None or wo.status != WorkOrderStatus.PLANNED:
            return None

        before = wo._snapshot()
        wo.status = WorkOrderStatus.IN_PROGRESS
        wo.actual_start = _now_iso()
        wo.updated_at = _now_iso()
        return wo.to_cdc_payload(op="u", before=before)

    def complete_work_order(self, wo_id: str) -> dict[str, Any] | None:
        """Complete a work order (all actions must be done)."""
        wo = self.work_orders.get(wo_id)
        if wo is None or wo.status != WorkOrderStatus.IN_PROGRESS:
            return None

        # Check all actions are completed
        actions = [a for a in self.maintenance_actions.values() if a.work_order_id == wo_id]
        if actions and not all(a.status == MaintenanceActionStatus.COMPLETED for a in actions):
            return None

        before = wo._snapshot()
        wo.status = WorkOrderStatus.COMPLETED
        wo.actual_end = _now_iso()
        wo.updated_at = _now_iso()
        return wo.to_cdc_payload(op="u", before=before)

    # ── Maintenance action lifecycle ─────────────────────

    def create_maintenance_actions(self, wo_id: str) -> list[dict[str, Any]]:
        """Create a set of maintenance action steps for a work order."""
        wo = self.work_orders.get(wo_id)
        if wo is None:
            return []

        asset = self.assets.get(wo.asset_id)
        templates = ACTION_TEMPLATES.get(
            asset.asset_type if asset else "pump", ACTION_TEMPLATES["pump"]
        )
        # Pick 2-4 steps from the template
        n_steps = self.rng.randint(2, min(4, len(templates)))
        selected = self.rng.sample(templates, n_steps)

        events: list[dict[str, Any]] = []
        for step_num, desc in enumerate(selected, start=1):
            action_id = self._new_id()
            action = MaintenanceAction(
                id=action_id,
                work_order_id=wo_id,
                step_number=step_num,
                description=desc,
            )
            self.maintenance_actions[action_id] = action
            events.append(action.to_cdc_payload(op="c"))
        return events

    def execute_maintenance_action(self, action_id: str) -> list[dict[str, Any]]:
        """Start and then complete a maintenance action (two events)."""
        action = self.maintenance_actions.get(action_id)
        if action is None or action.status != MaintenanceActionStatus.PENDING:
            return []

        events: list[dict[str, Any]] = []

        # Start
        before = action._snapshot()
        action.status = MaintenanceActionStatus.IN_PROGRESS
        action.started_at = _now_iso()
        events.append(action.to_cdc_payload(op="u", before=before))

        # Complete
        before = action._snapshot()
        action.status = MaintenanceActionStatus.COMPLETED
        action.completed_at = _now_iso()
        events.append(action.to_cdc_payload(op="u", before=before))

        return events

    # ── Batch generation ─────────────────────────────────

    def generate_batch(self, n_events: int) -> list[dict[str, Any]]:
        """Generate a batch of CDC events simulating realistic lifecycle flows.

        The generator follows the natural lifecycle:
        1. Create assets
        2. Degrade some assets
        3. Raise work requests
        4. Approve work requests → create work orders
        5. Execute maintenance actions
        6. Complete work orders → restore assets

        Args:
            n_events: Approximate number of events to generate.

        Returns:
            List of CDC event dictionaries.
        """
        events: list[dict[str, Any]] = []

        # Phase 1: Create some assets (≈20% of events)
        n_assets = max(3, n_events // 5)
        for _ in range(n_assets):
            events.append(self.create_asset())

        # Phase 2: Degrade some assets (≈50% of existing)
        degradable = [
            aid for aid, a in self.assets.items() if a.status == AssetStatus.OPERATIONAL
        ]
        n_degrade = max(1, len(degradable) // 2)
        for aid in self.rng.sample(degradable, min(n_degrade, len(degradable))):
            event = self.degrade_asset(aid)
            if event:
                events.append(event)

        # Phase 3: Raise work requests for degraded/failed assets
        requestable = [
            aid
            for aid, a in self.assets.items()
            if a.status in (AssetStatus.DEGRADED, AssetStatus.FAILED)
        ]
        for aid in requestable:
            event = self.create_work_request(aid)
            if event:
                events.append(event)

        # Phase 4: Approve work requests and create work orders
        approvable = [
            wrid for wrid, wr in self.work_requests.items() if wr.status == WorkRequestStatus.OPEN
        ]
        for wrid in approvable:
            event = self.approve_work_request(wrid)
            if event:
                events.append(event)
            event = self.create_work_order(wrid)
            if event:
                events.append(event)

        # Phase 5: Start work orders and create/execute maintenance actions
        startable = [
            woid
            for woid, wo in self.work_orders.items()
            if wo.status == WorkOrderStatus.PLANNED
        ]
        for woid in startable:
            event = self.start_work_order(woid)
            if event:
                events.append(event)

            # Create and execute actions
            action_events = self.create_maintenance_actions(woid)
            events.extend(action_events)

            wo_actions = [
                aid
                for aid, a in self.maintenance_actions.items()
                if a.work_order_id == woid and a.status == MaintenanceActionStatus.PENDING
            ]
            for aid in wo_actions:
                events.extend(self.execute_maintenance_action(aid))

        # Phase 6: Complete work orders and restore assets
        completable = [
            woid
            for woid, wo in self.work_orders.items()
            if wo.status == WorkOrderStatus.IN_PROGRESS
        ]
        for woid in completable:
            wo = self.work_orders[woid]
            event = self.complete_work_order(woid)
            if event:
                events.append(event)
                # Close the work request
                close_event = self.close_work_request(wo.work_request_id)
                if close_event:
                    events.append(close_event)
                # Restore the asset
                restore_event = self.restore_asset(wo.asset_id)
                if restore_event:
                    events.append(restore_event)

        return events[:n_events] if len(events) > n_events else events
