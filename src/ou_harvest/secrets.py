from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from .config import ProviderConfig


class SecretStore:
    def __init__(self, path: Path | None = None):
        self.path = path or default_secrets_path()

    def load(self) -> dict:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def save(self, payload: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def get(self, provider_name: str, key: str = "api_key") -> str | None:
        payload = self.load()
        value = payload.get(provider_name, {}).get(key)
        return value or None

    def set(self, provider_name: str, value: str, key: str = "api_key") -> None:
        payload = self.load()
        provider_payload = payload.setdefault(provider_name, {})
        provider_payload[key] = value
        self.save(payload)

    def clear(self, provider_name: str, key: str = "api_key") -> None:
        payload = self.load()
        if provider_name in payload:
            payload[provider_name].pop(key, None)
            if not payload[provider_name]:
                payload.pop(provider_name, None)
        self.save(payload)

    def has(self, provider_name: str, key: str = "api_key") -> bool:
        return self.get(provider_name, key) is not None


def default_app_config_dir() -> Path:
    home = Path.home()
    if sys.platform == "darwin":
        return home / "Library" / "Application Support" / "ou-harvest"
    if os.name == "nt":
        base = Path(os.getenv("APPDATA", home / "AppData" / "Roaming"))
        return base / "ou-harvest"
    base = Path(os.getenv("XDG_CONFIG_HOME", home / ".config"))
    return base / "ou-harvest"


def default_secrets_path() -> Path:
    return default_app_config_dir() / "secrets.json"


def resolve_provider_api_key(
    provider_name: str, provider: ProviderConfig, secret_store: SecretStore | None = None
) -> tuple[str | None, str | None]:
    env_name = provider.api_key_env
    if env_name:
        env_value = os.getenv(env_name)
        if env_value:
            return env_value, f"env:{env_name}"
    store = secret_store or SecretStore()
    secret = store.get(provider_name)
    if secret:
        return secret, f"secret:{store.path}"
    return None, None
