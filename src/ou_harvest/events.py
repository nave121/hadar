from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable


def event_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(slots=True)
class RunEvent:
    kind: str
    stage: str | None = None
    message: str | None = None
    data: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=event_timestamp)


EventSink = Callable[[RunEvent], None]


class RunCancelled(RuntimeError):
    """Raised when a running stage is cancelled by the caller."""
