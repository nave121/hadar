from __future__ import annotations

import json
import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)

from .config import AppConfig, ProviderConfig
from .secrets import SecretStore, resolve_provider_api_key

EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "current_rank": {"type": ["string", "null"]},
        "research_interests": {"type": "array", "items": {"type": "string"}},
        "education": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "degree_level": {"type": ["string", "null"]},
                    "field": {"type": ["string", "null"]},
                    "institution": {"type": ["string", "null"]},
                    "start_date": {"type": ["string", "null"]},
                    "end_date": {"type": ["string", "null"]},
                    "year": {"type": ["integer", "null"]},
                    "excerpt": {"type": ["string", "null"]},
                    "confidence": {"type": "number"},
                },
            },
        },
        "appointments": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": ["string", "null"]},
                    "institution": {"type": ["string", "null"]},
                    "department": {"type": ["string", "null"]},
                    "faculty": {"type": ["string", "null"]},
                    "start_date": {"type": ["string", "null"]},
                    "end_date": {"type": ["string", "null"]},
                    "is_current": {"type": ["boolean", "null"]},
                    "excerpt": {"type": ["string", "null"]},
                    "confidence": {"type": "number"},
                },
            },
        },
        "publications": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": ["string", "null"]},
                    "venue": {"type": ["string", "null"]},
                    "year": {"type": ["integer", "null"]},
                    "authors_text": {"type": ["string", "null"]},
                    "publication_type": {"type": ["string", "null"]},
                    "doi_or_url": {"type": ["string", "null"]},
                    "excerpt": {"type": ["string", "null"]},
                    "confidence": {"type": "number"},
                },
            },
        },
        "awards": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": ["string", "null"]},
                    "organization": {"type": ["string", "null"]},
                    "year": {"type": ["integer", "null"]},
                    "excerpt": {"type": ["string", "null"]},
                    "confidence": {"type": "number"},
                },
            },
        },
        "academic_service": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "role": {"type": ["string", "null"]},
                    "organization": {"type": ["string", "null"]},
                    "start_date": {"type": ["string", "null"]},
                    "end_date": {"type": ["string", "null"]},
                    "excerpt": {"type": ["string", "null"]},
                    "confidence": {"type": "number"},
                },
            },
        },
        "notable_links": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "kind": {"type": ["string", "null"]},
                    "url": {"type": ["string", "null"]},
                    "label": {"type": ["string", "null"]},
                    "excerpt": {"type": ["string", "null"]},
                    "confidence": {"type": "number"},
                },
            },
        },
    },
}


class BaseExtractor:
    def __init__(self, provider: ProviderConfig):
        self.provider = provider

    def extract(self, text: str, *, source_kind: str = "profile") -> dict[str, Any]:
        raise NotImplementedError

    @staticmethod
    def system_prompt() -> str:
        return (
            "You extract structured public academic profile data into strict JSON.\n\n"
            "Extract only public professional academic information. "
            "Return strict JSON matching the provided schema. "
            "Do not infer hidden or sensitive personal traits. "
            "Use null for unknown values and include only evidence that appears in the text. "
            "Avoid private personal details even if present in a CV.\n\n"
            f"Schema:\n{json.dumps(EXTRACTION_SCHEMA, ensure_ascii=False)}"
        )

    @staticmethod
    def build_user_prompt(text: str, *, source_kind: str = "profile") -> str:
        if source_kind == "cv":
            source_instructions = (
                "The text below comes from a curriculum vitae. Extract public professional and academic details "
                "including education, appointments, publications, awards, academic service, research areas, and notable professional links. "
                "Do not overwrite current university contact or unit data unless the text only fills missing history."
            )
        else:
            source_instructions = (
                "The text below comes from a public staff profile or directory page. Extract public professional academic details "
                "such as rank, education, appointments, research areas, and notable professional links."
            )
        return f"Source guidance:\n{source_instructions}\n\nText:\n{text}"

    @staticmethod
    def build_prompt(text: str, *, source_kind: str = "profile") -> str:
        """Full prompt with schema included (for Ollama single-prompt mode)."""
        return (
            BaseExtractor.system_prompt() + "\n\n"
            + BaseExtractor.build_user_prompt(text, source_kind=source_kind)
        )


class OllamaExtractor(BaseExtractor):
    def extract(self, text: str, *, source_kind: str = "profile") -> dict[str, Any]:
        response = requests.post(
            f"{self.provider.base_url.rstrip('/')}/api/generate",
            json={
                "model": self.provider.model,
                "prompt": self.build_prompt(text, source_kind=source_kind),
                "stream": False,
                "format": "json",
            },
            timeout=300,
        )
        response.raise_for_status()
        payload = response.json()
        try:
            return json.loads(payload["response"])
        except json.JSONDecodeError:
            logger.warning("Ollama returned invalid JSON for model %s: %.200s", self.provider.model, payload.get("response", ""))
            return {}


class OpenAIExtractor(BaseExtractor):
    def __init__(self, provider: ProviderConfig, api_key: str):
        super().__init__(provider)
        self.api_key = api_key

    def extract(self, text: str, *, source_kind: str = "profile") -> dict[str, Any]:
        response = requests.post(
            f"{self.provider.base_url.rstrip('/')}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={
                "model": self.provider.model,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": self.system_prompt()},
                    {"role": "user", "content": self.build_user_prompt(text, source_kind=source_kind)},
                ],
            },
            timeout=300,
        )
        response.raise_for_status()
        payload = response.json()
        content = payload["choices"][0]["message"]["content"]
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            logger.warning("OpenAI returned invalid JSON for model %s: %.200s", self.provider.model, content)
            return {}


def build_extractor(
    config: AppConfig, provider_name: str, secret_store: SecretStore | None = None
) -> BaseExtractor:
    if provider_name == "ollama":
        return OllamaExtractor(config.ollama)
    if provider_name == "openai":
        api_key, _source = resolve_provider_api_key("openai", config.openai, secret_store=secret_store)
        if not api_key:
            env_name = config.openai.api_key_env or "OPENAI_API_KEY"
            raise RuntimeError(f"Missing OpenAI API key in env or secret store ({env_name})")
        return OpenAIExtractor(config.openai, api_key)
    raise ValueError(f"Unsupported provider: {provider_name}")
