"""WorkOrder entity — approved and scheduled maintenance work.

Lifecycle: planned → in_progress → completed | cancelled
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class WorkOrderStatus(str, Enum):
    """Status of a work order."""

    PLANNED = "planned"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass
class WorkOrder:
    """Approved maintenance work linked to a work request and asset."""

    id: str
    work_request_id: str
    asset_id: str
    status: WorkOrderStatus = WorkOrderStatus.PLANNED
    planned_start: str = ""  # ISO-8601 timestamp
    planned_end: str = ""  # ISO-8601 timestamp
    actual_start: str = ""  # ISO-8601 timestamp (set when work begins)
    actual_end: str = ""  # ISO-8601 timestamp (set when work completes)
    updated_at: str = ""  # ISO-8601 timestamp

    def _snapshot(self) -> dict[str, Any]:
        """Current state as a flat dictionary."""
        return {
            "id": self.id,
            "work_request_id": self.work_request_id,
            "asset_id": self.asset_id,
            "status": self.status.value,
            "planned_start": self.planned_start,
            "planned_end": self.planned_end,
            "actual_start": self.actual_start,
            "actual_end": self.actual_end,
            "updated_at": self.updated_at,
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
            "entity": "WorkOrder",
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
