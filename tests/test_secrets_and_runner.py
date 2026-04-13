from __future__ import annotations

from pathlib import Path

from ou_harvest.config import AppConfig
from ou_harvest.events import RunEvent
from ou_harvest.runner import PipelineRunner
from ou_harvest.secrets import SecretStore, resolve_provider_api_key


def test_secret_resolution_prefers_env(monkeypatch, tmp_path: Path):
    store = SecretStore(tmp_path / "secrets.json")
    store.set("openai", "secret-file-token")
    config = AppConfig()
    config.openai.api_key_env = "OPENAI_API_KEY"
    monkeypatch.setenv("OPENAI_API_KEY", "env-token")

    token, source = resolve_provider_api_key("openai", config.openai, secret_store=store)

    assert token == "env-token"
    assert source == "env:OPENAI_API_KEY"


def test_secret_resolution_uses_local_secret_when_env_missing(tmp_path: Path):
    store = SecretStore(tmp_path / "secrets.json")
    store.set("openai", "secret-file-token")
    config = AppConfig()
    config.openai.api_key_env = "OPENAI_API_KEY"

    token, source = resolve_provider_api_key("openai", config.openai, secret_store=store)

    assert token == "secret-file-token"
    assert source == f"secret:{store.path}"


def test_config_save_round_trip(tmp_path: Path):
    config = AppConfig(
        seed_result_urls=["https://example.com/a", "https://example.com/b"],
        output_root="custom-data",
        selected_filters_by_connector={
            "openu": {"unit": ["307"], "staff_type": ["10"]},
            "bgu": {"unit": ["117531"], "campus": ["5"]},
        },
    )
    config.ollama.enabled = True
    config.openai.api_key_env = "MY_OPENAI_KEY"
    config.demographics.enabled = True
    config.demographics.detector_backend = "mtcnn"
    path = tmp_path / "ou_harvest.toml"

    config.save(path)
    loaded = AppConfig.load(path)

    assert loaded.seed_result_urls == ["https://example.com/a", "https://example.com/b"]
    assert loaded.output_root == "custom-data"
    assert loaded.selected_filters_by_connector == {
        "openu": {"unit": ["307"], "staff_type": ["10"]},
        "bgu": {"unit": ["117531"], "campus": ["5"]},
    }
    assert loaded.ollama.enabled is True
    assert loaded.openai.api_key_env == "MY_OPENAI_KEY"
    assert loaded.demographics.enabled is True
    assert loaded.demographics.detector_backend == "mtcnn"
    assert loaded.source_path == path


def test_config_save_round_trip_none_api_key_env(tmp_path: Path):
    config = AppConfig()
    config.openai.api_key_env = None
    path = tmp_path / "ou_harvest.toml"

    config.save(path)
    loaded = AppConfig.load(path)

    assert loaded.openai.api_key_env is None


def test_config_load_migrates_legacy_unit_and_staff_filters(tmp_path: Path):
    path = tmp_path / "ou_harvest.toml"
    path.write_text(
        "\n".join(
            [
                'university = "openu"',
                'start_url = "https://www.openu.ac.il/staff/pages/default.aspx"',
                'seed_result_urls = []',
                'allowed_domains = ["openu.ac.il"]',
                "request_timeout_seconds = 30.0",
                "throttle_seconds = 0.5",
                'user_agent = "ou-harvest/0.1"',
                'output_root = "data"',
                "personal_page_limit = 0",
                'selected_units = ["307"]',
                'selected_staff_types = ["10", "20"]',
                "",
                "[ollama]",
                "enabled = false",
                'base_url = "http://localhost:11434"',
                'model = "llama3.1:8b"',
                "",
                "[openai]",
                "enabled = false",
                'base_url = "https://api.openai.com/v1"',
                'model = "gpt-4.1-mini"',
                'api_key_env = "OPENAI_API_KEY"',
                "",
                "[review]",
                "confidence_threshold = 0.78",
                "",
            ]
        ),
        encoding="utf-8",
    )

    loaded = AppConfig.load(path)

    assert loaded.selected_filters_for_connector("openu") == {
        "unit": ["307"],
        "staff_type": ["10", "20"],
    }


def test_runner_emits_stage_lifecycle_events():
    events: list[RunEvent] = []

    class FakePipeline:
        def __init__(self, config, *, event_sink=None, should_cancel=None, secret_store=None):
            self.event_sink = event_sink

        def discover(self):
            if self.event_sink is not None:
                self.event_sink(
                    RunEvent(
                        kind="progress",
                        stage="discover",
                        message="fake progress",
                        data={"url": "https://example.com"},
                    )
                )
            return type(
                "DiscoveryResult",
                (),
                {"result_links": [1, 2], "department_staff_links": [1]},
            )()

    runner = PipelineRunner(AppConfig(), pipeline_factory=FakePipeline, event_sink=events.append)
    runner.discover()

    kinds = [event.kind for event in events]
    assert kinds[0] == "stage_started"
    assert "progress" in kinds
    assert kinds[-1] == "stage_completed"


def test_runner_emits_demographics_stage_lifecycle_events():
    events: list[RunEvent] = []

    class FakePipeline:
        def __init__(self, config, *, event_sink=None, should_cancel=None, secret_store=None):
            self.event_sink = event_sink

        def analyze_demographics(self):
            if self.event_sink is not None:
                self.event_sink(
                    RunEvent(
                        kind="progress",
                        stage="demographics",
                        message="fake progress",
                        data={"person_id": "abc123"},
                    )
                )
            return [1, 2]

    runner = PipelineRunner(AppConfig(), pipeline_factory=FakePipeline, event_sink=events.append)
    runner.analyze_demographics()

    kinds = [event.kind for event in events]
    assert kinds[0] == "stage_started"
    assert "progress" in kinds
    assert kinds[-1] == "stage_completed"


def test_runner_doctor_reports_secret_source(tmp_path: Path):
    store = SecretStore(tmp_path / "secrets.json")
    store.set("openai", "secret-file-token")
    config = AppConfig()

    report = PipelineRunner(config, secret_store=store).doctor()

    assert report["providers"]["openai"]["api_key_available"] is True
    assert report["providers"]["openai"]["api_key_source"] == f"secret:{store.path}"
