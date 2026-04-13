from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("textual")

from textual.widgets import Checkbox, Input, Static

from ou_harvest.config import AppConfig
from ou_harvest.models import DemographicEstimate, PersonRecord
from ou_harvest.storage import Storage
from ou_harvest.tui import OuHarvestTUI


def _save_config(tmp_path: Path, *, enabled: bool = False, detector_backend: str = "retinaface") -> Path:
    config = AppConfig(output_root=str(tmp_path / "data"))
    config.demographics.enabled = enabled
    config.demographics.detector_backend = detector_backend
    path = tmp_path / "ou_harvest.toml"
    config.save(path)
    return path


@pytest.mark.asyncio
async def test_tui_includes_demographics_button_and_config_controls(tmp_path: Path):
    app = OuHarvestTUI(config_path=_save_config(tmp_path))

    async with app.run_test():
        button_ids = [button.id for button in app.query("#run_buttons Button")]
        assert button_ids.index("run_parse") < button_ids.index("run_demographics")
        assert button_ids.index("run_demographics") < button_ids.index("run_enrich_ollama")
        assert isinstance(app.query_one("#demographics_enabled", Checkbox), Checkbox)
        detector_backend = app.query_one("#demographics_detector_backend", Input)
        assert detector_backend.value == "retinaface"


@pytest.mark.asyncio
async def test_tui_writes_demographics_config_into_state(tmp_path: Path):
    app = OuHarvestTUI(config_path=_save_config(tmp_path))

    async with app.run_test():
        app.query_one("#demographics_enabled", Checkbox).value = True
        app.query_one("#demographics_detector_backend", Input).value = "mtcnn"

        assert app._write_config_into_state() is True
        assert app.config.demographics.enabled is True
        assert app.config.demographics.detector_backend == "mtcnn"


@pytest.mark.asyncio
async def test_tui_demographics_button_starts_background_command(tmp_path: Path, monkeypatch):
    app = OuHarvestTUI(config_path=_save_config(tmp_path))
    calls: list[tuple[str, dict]] = []

    async with app.run_test() as pilot:
        monkeypatch.setattr(app, "_start_background", lambda command, **kwargs: calls.append((command, kwargs)))
        await pilot.click("#run_demographics")

    assert calls == [("demographics", {})]


@pytest.mark.asyncio
async def test_tui_run_command_thread_dispatches_demographics(tmp_path: Path, monkeypatch):
    app = OuHarvestTUI(config_path=_save_config(tmp_path))
    calls: list[str] = []

    class FakeRunner:
        def analyze_demographics(self):
            calls.append("demographics")

    async with app.run_test():
        app.active_runner = FakeRunner()
        monkeypatch.setattr(app, "call_from_thread", lambda func, *args, **kwargs: func(*args, **kwargs))
        monkeypatch.setattr(app, "_set_running", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(app, "refresh_data", lambda: None)
        monkeypatch.setattr(app, "_populate_filter_lists", lambda: None)
        monkeypatch.setattr(app, "_refresh_filter_summary", lambda: None)

        app._run_command_thread("demographics")

    assert calls == ["demographics"]


@pytest.mark.asyncio
async def test_tui_summary_shows_image_count_and_demographics_config(tmp_path: Path, monkeypatch):
    app = OuHarvestTUI(config_path=_save_config(tmp_path, enabled=True, detector_backend="mtcnn"))

    async with app.run_test():
        monkeypatch.setattr(
            "ou_harvest.tui.PipelineRunner.snapshot",
            lambda self: {
                "records_count": 5,
                "raw_html_count": 7,
                "raw_pdf_count": 2,
                "raw_image_count": 3,
                "review_queue_count": 1,
                "crawl_manifest_count": 9,
                "discovered_result_links": 4,
                "exports": [],
            },
        )
        monkeypatch.setattr(
            "ou_harvest.tui.resolve_provider_api_key",
            lambda *_args, **_kwargs: ("token", "env:OPENAI_API_KEY"),
        )

        app.status.update({"running": False, "stage": "demographics", "message": "Completed demographics"})
        app._refresh_summary()
        summary = str(app.query_one("#summary", Static).renderable)

    assert "Images: 3" in summary
    assert "Demographics: ON (mtcnn)" in summary


@pytest.mark.asyncio
async def test_tui_records_table_renders_demographics_and_photo_status(tmp_path: Path, monkeypatch):
    config_path = _save_config(tmp_path)
    storage = Storage(tmp_path / "data")
    storage.save_record(
        PersonRecord(
            person_id="abc123",
            full_name="Dr. Test Person",
            photo_url="https://example.com/photo.jpg",
            photo_artifact_id="photo123",
            demographics=DemographicEstimate(
                dominant_gender="Woman",
                dominant_race="white",
                estimated_age=41,
            ),
        )
    )
    app = OuHarvestTUI(config_path=config_path)

    async with app.run_test():
        table = app.query_one("#records")
        rows: list[tuple] = []
        monkeypatch.setattr(table, "add_row", lambda *args: rows.append(args))
        app._refresh_records_table()

    assert rows
    assert rows[0][1] == "Woman"
    assert rows[0][2] == "white"
    assert rows[0][3] == "41"
    assert rows[0][4] == "yes"
