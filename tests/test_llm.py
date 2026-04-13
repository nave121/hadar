from __future__ import annotations

from unittest.mock import patch, MagicMock

from ou_harvest.config import ProviderConfig
from ou_harvest.llm import OllamaExtractor, OpenAIExtractor


def _mock_response(json_data):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = json_data
    return resp


def test_ollama_extractor_returns_empty_on_invalid_json():
    provider = ProviderConfig(enabled=True, base_url="http://localhost:11434", model="test")
    extractor = OllamaExtractor(provider)

    with patch("ou_harvest.llm.requests.post", return_value=_mock_response({"response": "not valid json {{{"})):
        result = extractor.extract("some text")

    assert result == {}


def test_openai_extractor_returns_empty_on_invalid_json():
    provider = ProviderConfig(enabled=True, base_url="https://api.openai.com/v1", model="test")
    extractor = OpenAIExtractor(provider, api_key="fake-key")

    payload = {"choices": [{"message": {"content": "not valid json {{{"}}]}
    with patch("ou_harvest.llm.requests.post", return_value=_mock_response(payload)):
        result = extractor.extract("some text")

    assert result == {}
