from __future__ import annotations

from .base import UniversityAdapter


def get_adapter(name: str) -> UniversityAdapter:
    """Get a university adapter by name."""
    adapters = _load_adapters()
    if name not in adapters:
        available = ", ".join(sorted(adapters.keys()))
        raise ValueError(f"Unknown university adapter: {name!r}. Available: {available}")
    return adapters[name]()


def available_adapters() -> dict[str, str]:
    """Return {name: display_name} for all registered adapters."""
    return {name: cls.display_name for name, cls in _load_adapters().items()}


def _load_adapters() -> dict[str, type[UniversityAdapter]]:
    from .openu import OpenUniversityAdapter
    from .bgu import BguAdapter
    from .technion_med import TechnionMedAdapter

    return {
        "openu": OpenUniversityAdapter,
        "bgu": BguAdapter,
        "technion_med": TechnionMedAdapter,
    }
