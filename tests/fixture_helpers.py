from __future__ import annotations

from pathlib import Path

import pytest


FIXTURES = Path(__file__).parent / "fixtures"


def require_fixture(name: str) -> Path:
    path = FIXTURES / name
    if not path.exists():
        pytest.skip(
            f"Missing generated fixture {name}. Run `python scripts/fetch_test_fixtures.py` first."
        )
    return path
