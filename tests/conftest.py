"""Shared test fixtures for the EAM Data Pipe PoC."""

from __future__ import annotations

import pytest

from eam_simulator.event_generator import EAMSimulator


@pytest.fixture
def simulator() -> EAMSimulator:
    """Return a deterministic EAM simulator (seed=42)."""
    return EAMSimulator(seed=42)


@pytest.fixture
def sample_events(simulator: EAMSimulator) -> list[dict]:
    """Generate a batch of sample CDC events for testing."""
    return simulator.generate_batch(50)
