"""MaintenanceAction entity — an executed step within a maintenance task.

Lifecycle: pending → in_progress → completed
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class MaintenanceActionStatus(str, Enum):
    """Status of a maintenance action step."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


@dataclass
class MaintenanceAction:
    """An atomic work step executed as part of a work order."""

    id: str
    work_order_id: str
    step_number: int
    description: str
    status: MaintenanceActionStatus = MaintenanceActionStatus.PENDING
    started_at: str = ""  # ISO-8601 timestamp
    completed_at: str = ""  # ISO-8601 timestamp

    def _snapshot(self) -> dict[str, Any]:
        """Current state as a flat dictionary."""
        return {
            "id": self.id,
            "work_order_id": self.work_order_id,
            "step_number": self.step_number,
            "description": self.description,
            "status": self.status.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }

    def to_cdc_payload(
        self,
        op: str,
        before: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a CDC envelope per AGENT.md §4.3."""
        now = datetime.now(timezone.utc).isoformat()
        after = None if op == "d" else self._snapshot()
        return {
            "entity": "MaintenanceAction",
            "op": op,
            "event_ts": now,
            "pk": {"id": self.id},
            "before": before or {},
            "after": after or {},
            "source": {
                "system": "simulated-eam",
                "version": "poc-v1",
            },
        }
