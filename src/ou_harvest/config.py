from __future__ import annotations

import json
from pathlib import Path
import tomllib

from pydantic import BaseModel, Field, PrivateAttr, field_validator, model_validator


class ProviderConfig(BaseModel):
    enabled: bool = False
    base_url: str
    model: str
    api_key_env: str | None = None

    @field_validator("api_key_env", mode="before")
    @classmethod
    def _coerce_empty_to_none(cls, v):
        if v == "":
            return None
        return v


class ReviewConfig(BaseModel):
    confidence_threshold: float = 0.78


class DemographicsConfig(BaseModel):
    enabled: bool = False
    detector_backend: str = "retinaface"


class AppConfig(BaseModel):
    university: str = "openu"
    start_url: str = "https://www.openu.ac.il/staff/pages/default.aspx"
    seed_result_urls: list[str] = Field(default_factory=list)
    allowed_domains: list[str] = Field(
        default_factory=lambda: ["openu.ac.il", "academic.openu.ac.il"]
    )
    request_timeout_seconds: float = 30.0
    throttle_seconds: float = 0.5
    user_agent: str = "ou-harvest/0.1"
    output_root: str = "data"
    personal_page_limit: int = 0
    selected_filters_by_connector: dict[str, dict[str, list[str]]] = Field(default_factory=dict)
    ollama: ProviderConfig = Field(
        default_factory=lambda: ProviderConfig(
            enabled=False, base_url="http://localhost:11434", model="llama3.1:8b"
        )
    )
    openai: ProviderConfig = Field(
        default_factory=lambda: ProviderConfig(
            enabled=False,
            base_url="https://api.openai.com/v1",
            model="gpt-4.1-mini",
            api_key_env="OPENAI_API_KEY",
        )
    )
    demographics: DemographicsConfig = Field(default_factory=DemographicsConfig)
    review: ReviewConfig = Field(default_factory=ReviewConfig)
    _source_path: Path | None = PrivateAttr(default=None)

    @property
    def output_path(self) -> Path:
        return Path(self.output_root)

    @property
    def source_path(self) -> Path | None:
        return self._source_path

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_filter_fields(cls, data):
        if not isinstance(data, dict):
            return data

        selected_filters = data.get("selected_filters_by_connector")
        if selected_filters:
            return data

        selected_units = [str(item) for item in data.get("selected_units", []) if str(item)]
        selected_staff_types = [str(item) for item in data.get("selected_staff_types", []) if str(item)]
        if not selected_units and not selected_staff_types:
            return data

        connector = str(data.get("university") or "openu")
        connector_filters: dict[str, list[str]] = {}
        if selected_units:
            connector_filters["unit"] = selected_units
        if selected_staff_types:
            connector_filters["staff_type"] = selected_staff_types
        if connector_filters:
            data["selected_filters_by_connector"] = {connector: connector_filters}
        return data

    @classmethod
    def load(cls, path: str | Path | None = None) -> "AppConfig":
        if path is None:
            candidate = Path("ou_harvest.toml")
            if candidate.exists():
                path = candidate
        if path is None:
            return cls()
        data = tomllib.loads(Path(path).read_text(encoding="utf-8"))
        config = cls.model_validate(data)
        config._source_path = Path(path)
        return config

    def save(self, path: str | Path | None = None) -> Path:
        target = Path(path) if path is not None else self._source_path or Path("ou_harvest.toml")
        target.write_text(self.to_toml(), encoding="utf-8")
        self._source_path = target
        return target

    def selected_filters_for_connector(self, connector: str | None = None) -> dict[str, list[str]]:
        key = connector or self.university
        connector_filters = self.selected_filters_by_connector.get(key, {})
        return {
            group_key: [str(item) for item in values]
            for group_key, values in connector_filters.items()
        }

    def set_selected_filters_for_connector(self, connector: str, filters: dict[str, list[str]]) -> None:
        normalized = {
            group_key: [str(item) for item in values if str(item)]
            for group_key, values in filters.items()
            if any(str(item) for item in values)
        }
        if normalized:
            self.selected_filters_by_connector[connector] = normalized
        else:
            self.selected_filters_by_connector.pop(connector, None)

    def to_toml(self) -> str:
        lines = [
            f"university = {_toml_value(self.university)}",
            f"start_url = {_toml_value(self.start_url)}",
            f"seed_result_urls = {_toml_value(self.seed_result_urls)}",
            f"allowed_domains = {_toml_value(self.allowed_domains)}",
            f"request_timeout_seconds = {_toml_value(self.request_timeout_seconds)}",
            f"throttle_seconds = {_toml_value(self.throttle_seconds)}",
            f"user_agent = {_toml_value(self.user_agent)}",
            f"output_root = {_toml_value(self.output_root)}",
            f"personal_page_limit = {_toml_value(self.personal_page_limit)}",
            f"selected_filters_by_connector = {_toml_value(self.selected_filters_by_connector)}",
            "",
            "[ollama]",
            f"enabled = {_toml_value(self.ollama.enabled)}",
            f"base_url = {_toml_value(self.ollama.base_url)}",
            f"model = {_toml_value(self.ollama.model)}",
            "",
            "[openai]",
            f"enabled = {_toml_value(self.openai.enabled)}",
            f"base_url = {_toml_value(self.openai.base_url)}",
            f"model = {_toml_value(self.openai.model)}",
            f"api_key_env = {_toml_value(self.openai.api_key_env)}",
            "",
            "[demographics]",
            f"enabled = {_toml_value(self.demographics.enabled)}",
            f"detector_backend = {_toml_value(self.demographics.detector_backend)}",
            "",
            "[review]",
            f"confidence_threshold = {_toml_value(self.review.confidence_threshold)}",
            "",
        ]
        return "\n".join(lines)


def _toml_value(value) -> str:
    if value is None:
        return json.dumps("")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    if isinstance(value, dict):
        items = ", ".join(f"{json.dumps(str(key))} = {_toml_value(item)}" for key, item in value.items())
        return "{ " + items + " }"
    return json.dumps(value)
