"""WorkRequest entity — a reported defect, issue, or maintenance need.

Lifecycle: open → approved | rejected → closed
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class WorkRequestStatus(str, Enum):
    """Status of a work request."""

    OPEN = "open"
    APPROVED = "approved"
    REJECTED = "rejected"
    CLOSED = "closed"


class WorkRequestPriority(str, Enum):
    """Priority level for a work request."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class WorkRequest:
    """A reported defect or maintenance need linked to an asset."""

    id: str
    asset_id: str
    description: str
    priority: WorkRequestPriority = WorkRequestPriority.MEDIUM
    status: WorkRequestStatus = WorkRequestStatus.OPEN
    reported_at: str = ""  # ISO-8601 timestamp
    updated_at: str = ""  # ISO-8601 timestamp

    def _snapshot(self) -> dict[str, Any]:
        """Current state as a flat dictionary."""
        return {
            "id": self.id,
            "asset_id": self.asset_id,
            "description": self.description,
            "priority": self.priority.value,
            "status": self.status.value,
            "reported_at": self.reported_at,
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
            "entity": "WorkRequest",
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
