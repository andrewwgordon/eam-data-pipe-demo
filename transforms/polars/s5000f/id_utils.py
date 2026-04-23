"""S5000F identity utilities — deterministic ID generation.

All S5000F entity IDs are derived from source primary keys using a
hash-based scheme. This ensures:
  - Reproducibility: same input always produces the same ID
  - Traceability: IDs encode the concept and source
  - No collisions across concept types (prefixed)
"""

from __future__ import annotations

import hashlib


def s5000f_id(concept: str, source_id: str) -> str:
    """Generate a deterministic S5000F identifier.

    Args:
        concept: S5000F concept name (e.g. 'ProductInstance', 'MaintenanceTask').
        source_id: Source system primary key.

    Returns:
        A deterministic, prefixed identifier string.
        Format: {concept_prefix}-{hash_12}
        Example: PI-a1b2c3d4e5f6
    """
    # Prefix mapping for readability
    prefixes: dict[str, str] = {
        "ProductInstance": "PI",
        "FunctionalFailure": "FF",
        "MaintenanceTask": "MT",
        "MaintenanceTaskStep": "MTS",
        "MaintenanceEvent": "ME",
    }
    prefix = prefixes.get(concept, concept[:3].upper())

    # Deterministic hash from concept + source_id
    hash_input = f"{concept}:{source_id}"
    hash_hex = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()[:12]

    return f"{prefix}-{hash_hex}"
