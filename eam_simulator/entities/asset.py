"""Asset entity — a physical plant equipment item.

Lifecycle: created → operational → degraded → failed → (repaired) → operational
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class AssetStatus(str, Enum):
    """Operational status of an asset."""

    OPERATIONAL = "operational"
    DEGRADED = "degraded"
    FAILED = "failed"


@dataclass
class Asset:
    """Represents a maintainable physical item in the plant."""

    id: str
    name: str
    asset_type: str
    location: str
    status: AssetStatus = AssetStatus.OPERATIONAL
    install_date: str = ""  # ISO-8601 date
    updated_at: str = ""  # ISO-8601 timestamp

    def _snapshot(self) -> dict[str, Any]:
        """Current state as a flat dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "asset_type": self.asset_type,
            "location": self.location,
            "status": self.status.value,
            "install_date": self.install_date,
            "updated_at": self.updated_at,
        }

    def to_cdc_payload(
        self,
        op: str,
        before: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a CDC envelope per AGENT.md §4.3.

        Args:
            op: Operation code — 'c' (create), 'u' (update), or 'd' (delete).
            before: Previous state snapshot (required for 'u' and 'd').

        Returns:
            CDC event dictionary ready for Kafka production.
        """
        now = datetime.now(timezone.utc).isoformat()
        after = None if op == "d" else self._snapshot()
        return {
            "entity": "Asset",
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
