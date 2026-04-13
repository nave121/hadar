from __future__ import annotations

from pathlib import Path
import threading
from typing import Any

import sys

if sys.platform == "win32":
    import io
    # Ensure stdout/stderr use UTF-8 to support Hebrew/Unicode on Windows
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", newline=None)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", newline=None)

from .adapters import available_adapters
from .config import AppConfig
from .events import RunEvent
from .runner import PipelineRunner
from .secrets import SecretStore, resolve_provider_api_key
from .storage import Storage


def _bidi_display(text: str) -> str:
    """Convert logical-order text to visual order for display in LTR terminals.

    Textual's DataTable renders all text LTR without any BiDi processing.
    This uses the Unicode BiDi algorithm (python-bidi) to produce visual-order
    strings that display correctly when laid out left-to-right.
    """
    if not text:
        return text
    try:
        from bidi.algorithm import get_display
        return get_display(text)
    except ImportError:
        return text


class OuHarvestTUI:
    """Lazy wrapper so importing this module without Textual still works."""

    def __new__(cls, *args, **kwargs):
        try:
            from textual.app import App, ComposeResult
            from textual.containers import Horizontal, Vertical, VerticalScroll
            from textual.widgets import (
                Button,
                Checkbox,
                Collapsible,
                DataTable,
                Footer,
                Header,
                Input,
                Label,
                ProgressBar,
                RichLog,
                Select,
                SelectionList,
                Static,
                TabbedContent,
                TabPane,
            )
        except ImportError as exc:
            raise RuntimeError("Textual is not installed. Install with `pip install -e .[tui]`.") from exc

        class _OuHarvestTextualApp(App[None]):
            CSS = """
            Screen {
              layout: vertical;
            }
            #run_buttons {
              height: auto;
              padding: 0 1;
            }
            #run_buttons Button {
              width: auto;
              min-width: 14;
              margin: 0 1 0 0;
            }
            #summary {
              height: auto;
              max-height: 20;
              border: round $accent;
              padding: 1;
              margin: 1 0;
            }
            #progress {
              margin: 0 1;
            }
            #log {
              height: 1fr;
              border: round $accent;
            }
            #records {
              height: 1fr;
            }
            #reviews {
              height: 1fr;
            }
            .table-title {
              text-style: bold;
              margin: 1 0 0 0;
            }
            #config_scroll {
              height: 1fr;
            }
            Collapsible {
              margin-bottom: 1;
            }
            .field-label {
              margin-top: 1;
              color: $text-muted;
            }
            Input {
              width: 1fr;
            }
            #config_actions {
              height: auto;
              padding: 1 0;
            }
            #config_actions Button {
              width: auto;
              min-width: 16;
              margin: 0 1 0 0;
            }
            .filter-selection-list {
              height: auto;
              max-height: 20;
            }
            .select-actions {
              height: auto;
              padding: 0 0 1 0;
            }
            .select-actions Button {
              width: auto;
              min-width: 12;
              margin: 0 1 0 0;
            }
            #filters_scroll {
              height: 1fr;
            }
            """

            BINDINGS = [
                ("q", "quit", "Quit"),
                ("r", "refresh_data", "Refresh"),
                ("c", "cancel_run", "Cancel"),
                ("1", "tab_run", "Run"),
                ("2", "tab_records", "Records"),
                ("3", "tab_config", "Config"),
                ("4", "tab_filters", "Filters"),
            ]

            def __init__(self, *, config_path: str | Path | None = None):
                super().__init__()
                self.config_path = Path(config_path) if config_path is not None else Path("ou_harvest.toml")
                self.config = AppConfig.load(self.config_path)
                self.secret_store = SecretStore()
                self.active_thread: threading.Thread | None = None
                self.active_runner: PipelineRunner | None = None
                self.status: dict[str, Any] = {"stage": None, "message": "Idle"}
                self._filter_render_generation = 0

            def compose(self) -> ComposeResult:
                yield Header()
                with TabbedContent(initial="tab_run"):
                    with TabPane("Run", id="tab_run"):
                        with Horizontal(id="run_buttons"):
                            yield Button("Full Run", id="run_full", variant="primary")
                            yield Button("Discover", id="run_discover")
                            yield Button("Crawl", id="run_crawl")
                            yield Button("Parse", id="run_parse")
                            yield Button("Enrich Ollama", id="run_enrich_ollama")
                            yield Button("Enrich OpenAI", id="run_enrich_openai")
                            yield Button("Review", id="run_review")
                            yield Button("Export", id="run_export")
                            yield Button("Cancel", id="cancel_run", variant="error")
                        yield Static("", id="summary")
                        yield ProgressBar(id="progress", total=100, show_eta=True)
                        yield RichLog(id="log", wrap=True, highlight=True, markup=False)

                    with TabPane("Records", id="tab_records"):
                        yield Static("Records", classes="table-title")
                        yield DataTable(id="records")
                        yield Static("Review Queue", classes="table-title")
                        yield DataTable(id="reviews")

                    with TabPane("Config", id="tab_config"):
                        with VerticalScroll(id="config_scroll"):
                            yield Label("University", classes="field-label")
                            adapters = available_adapters()
                            yield Select(
                                [(display, name) for name, display in adapters.items()],
                                value=self.config.university,
                                id="university",
                            )
                            yield Static("", id="config_filter_summary")
                            with Collapsible(title="Crawl Settings", collapsed=False):
                                yield Label("Start URL", classes="field-label")
                                yield Input(value=self.config.start_url, placeholder="Start URL", id="start_url")
                                yield Label("Allowed domains (comma-separated)", classes="field-label")
                                yield Input(value=",".join(self.config.allowed_domains), placeholder="Allowed domains", id="allowed_domains")
                                yield Label("Seed result URLs (comma-separated)", classes="field-label")
                                yield Input(value=",".join(self.config.seed_result_urls), placeholder="Seed result URLs", id="seed_result_urls")
                                yield Label("Output root", classes="field-label")
                                yield Input(value=self.config.output_root, placeholder="Output root", id="output_root")
                                yield Label("Throttle seconds", classes="field-label")
                                yield Input(value=str(self.config.throttle_seconds), placeholder="Throttle seconds", id="throttle_seconds")
                                yield Label("Timeout seconds", classes="field-label")
                                yield Input(value=str(self.config.request_timeout_seconds), placeholder="Timeout seconds", id="request_timeout_seconds")
                                yield Label("Personal page limit (0 = unlimited)", classes="field-label")
                                yield Input(value=str(self.config.personal_page_limit), placeholder="Personal page limit", id="personal_page_limit")
                            with Collapsible(title="Ollama", collapsed=True):
                                yield Checkbox("Enable Ollama", value=self.config.ollama.enabled, id="ollama_enabled")
                                yield Label("Base URL", classes="field-label")
                                yield Input(value=self.config.ollama.base_url, placeholder="Ollama base URL", id="ollama_base_url")
                                yield Label("Model", classes="field-label")
                                yield Input(value=self.config.ollama.model, placeholder="Ollama model", id="ollama_model")
                            with Collapsible(title="OpenAI", collapsed=True):
                                yield Checkbox("Enable OpenAI", value=self.config.openai.enabled, id="openai_enabled")
                                yield Label("Base URL", classes="field-label")
                                yield Input(value=self.config.openai.base_url, placeholder="OpenAI base URL", id="openai_base_url")
                                yield Label("Model", classes="field-label")
                                yield Input(value=self.config.openai.model, placeholder="OpenAI model", id="openai_model")
                                yield Label("API key env var", classes="field-label")
                                yield Input(value=self.config.openai.api_key_env or "OPENAI_API_KEY", placeholder="OpenAI env var", id="openai_api_key_env")
                                yield Label("API token (stored outside repo)", classes="field-label")
                                yield Input(value="", placeholder="OpenAI token", password=True, id="openai_token")
                            with Collapsible(title="Review", collapsed=True):
                                yield Label("Confidence threshold", classes="field-label")
                                yield Input(value=str(self.config.review.confidence_threshold), placeholder="Confidence threshold", id="confidence_threshold")
                            with Horizontal(id="config_actions"):
                                yield Button("Save Config", id="save_config", variant="success")
                                yield Button("Save Token", id="save_token", variant="success")
                                yield Button("Clear Token", id="clear_token")
                                yield Button("Doctor", id="doctor")
                                yield Button("Refresh", id="refresh_data")
                    with TabPane("Filters", id="tab_filters"):
                        with VerticalScroll(id="filters_scroll"):
                            yield Static("", id="filters_summary")
                            yield Static("Run Discover first to populate filter options", id="filter_hint")
                            yield Vertical(id="filter_groups")
                yield Footer()

            def _apply_tooltips(self) -> None:
                """Set tooltips on all buttons and config inputs."""
                tips: dict[str, str] = {
                    # Run buttons
                    "run_full": "Run all stages: discover, crawl, parse, enrich, review, export",
                    "run_discover": "Fetch the staff directory landing page and extract result links",
                    "run_crawl": "Follow result page links, fetch personal pages, CVs, and CRIS pages",
                    "run_parse": "Parse all downloaded HTML/PDF artifacts into PersonRecord JSON",
                    "run_enrich_ollama": "Enrich records using local Ollama LLM (education, appointments, etc.)",
                    "run_enrich_openai": "Enrich records using OpenAI API (education, appointments, etc.)",
                    "run_review": "Build the review queue from low-confidence records",
                    "run_export": "Export all records to JSON and JSONL in data/exports/",
                    "cancel_run": "Cancel the currently running pipeline stage",
                    # Config actions
                    "save_config": "Write current settings back to ou_harvest.toml",
                    "save_token": "Store the OpenAI token in the local secret store (not in the repo)",
                    "clear_token": "Remove the OpenAI token from the local secret store",
                    "doctor": "Check config, dependencies, and provider availability",
                    "refresh_data": "Reload records and review queue from disk",
                    # Config inputs
                    "start_url": "The staff directory landing page URL used for discovery",
                    "allowed_domains": "Only fetch URLs from these domains (comma-separated)",
                    "seed_result_urls": "Extra result page URLs to always include (comma-separated)",
                    "output_root": "Root directory for all pipeline output (default: data)",
                    "throttle_seconds": "Minimum delay between HTTP requests (default: 0.5)",
                    "request_timeout_seconds": "HTTP request timeout in seconds (default: 30)",
                    "personal_page_limit": "Max personal pages to fetch across the entire crawl (0 = unlimited)",
                    "ollama_enabled": "Enable local Ollama for LLM enrichment",
                    "ollama_base_url": "Ollama API endpoint (default: http://localhost:11434)",
                    "ollama_model": "Ollama model name for enrichment extraction",
                    "openai_enabled": "Enable OpenAI API for LLM enrichment",
                    "openai_base_url": "OpenAI-compatible API base URL",
                    "openai_model": "OpenAI model name for enrichment extraction",
                    "openai_api_key_env": "Environment variable name that holds the OpenAI API key",
                    "openai_token": "Paste an OpenAI API key here, then click Save Token to store it locally",
                    "confidence_threshold": "Records with confidence below this are flagged for review (default: 0.78)",
                    "university": "Select which university to scrape (openu, bgu, technion_med)",
                }
                for widget_id, tip in tips.items():
                    try:
                        self.query_one(f"#{widget_id}").tooltip = tip
                    except Exception:
                        pass

            def on_mount(self) -> None:
                self._configure_tables()
                self._apply_tooltips()
                self.refresh_data()
                self._refresh_summary()
                self._refresh_filter_summary()
                self._write_log("TUI ready")

            def on_button_pressed(self, event: Button.Pressed) -> None:
                button_id = event.button.id
                if button_id is None:
                    return
                handlers = {
                    "run_full": lambda: self._start_background("full_run"),
                    "run_discover": lambda: self._start_background("discover"),
                    "run_crawl": lambda: self._start_background("crawl"),
                    "run_parse": lambda: self._start_background("parse"),
                    "run_enrich_ollama": lambda: self._start_background("enrich", provider="ollama"),
                    "run_enrich_openai": lambda: self._start_background("enrich", provider="openai"),
                    "run_review": lambda: self._start_background("review"),
                    "run_export": self._run_export,
                    "cancel_run": self.action_cancel_run,
                    "save_config": self._save_config,
                    "save_token": self._save_token,
                    "clear_token": self._clear_token,
                    "refresh_data": self.refresh_data,
                    "doctor": self._run_doctor,
                }
                if button_id.startswith("select_all_filter__"):
                    self._select_all(button_id.removeprefix("select_all_filter__"))
                    return
                if button_id.startswith("clear_filter__"):
                    self._clear_all(button_id.removeprefix("clear_filter__"))
                    return
                handler = handlers.get(button_id)
                if handler is not None:
                    handler()

            def on_select_changed(self, event: Select.Changed) -> None:
                if event.select.id == "university" and event.value != Select.BLANK:
                    from .adapters import get_adapter
                    self.config.university = str(event.value)
                    adapter = get_adapter(str(event.value))
                    self.query_one("#start_url", Input).value = adapter.default_start_url
                    self.query_one("#seed_result_urls", Input).value = ""
                    allowed = ",".join(adapter.default_allowed_domains)
                    self.query_one("#allowed_domains", Input).value = allowed
                    self._populate_filter_lists()
                    self._refresh_filter_summary()
                    self._write_log(f"Switched to {adapter.display_name} — URLs and filters updated")

            def action_refresh_data(self) -> None:
                self.refresh_data()

            def action_cancel_run(self) -> None:
                if self.active_runner is not None:
                    self.active_runner.cancel()
                    self._write_log("Cancellation requested")

            def action_tab_run(self) -> None:
                self.query_one(TabbedContent).active = "tab_run"

            def action_tab_records(self) -> None:
                self.query_one(TabbedContent).active = "tab_records"

            def action_tab_config(self) -> None:
                self.query_one(TabbedContent).active = "tab_config"

            def action_tab_filters(self) -> None:
                self.query_one(TabbedContent).active = "tab_filters"

            def _active_discovery_state(self) -> dict[str, Any]:
                storage = Storage(self.config.output_path)
                discovery = storage.load_json("state/discovery.json", default={})
                connector_name = discovery.get("connector_name")
                if connector_name and connector_name != self.config.university:
                    return {}
                if "available_filters" not in discovery:
                    available_filters: list[dict[str, Any]] = []
                    available_units = discovery.get("available_units", [])
                    if available_units:
                        available_filters.append({"key": "unit", "label": "Departments", "options": available_units})
                    available_staff_types = discovery.get("available_staff_types", [])
                    if available_staff_types:
                        available_filters.append(
                            {"key": "staff_type", "label": "Staff Types", "options": available_staff_types}
                        )
                    discovery["available_filters"] = available_filters
                return discovery

            def _populate_filter_lists(self) -> None:
                """Load connector-specific discovery filters from the last matching discovery snapshot."""
                hint = self.query_one("#filter_hint", Static)
                container = self.query_one("#filter_groups", Vertical)
                container.remove_children()

                discovery = self._active_discovery_state()
                available_filters = discovery.get("available_filters", [])

                if not discovery:
                    hint.update("Run Discover for the active connector to load filter options")
                    return

                if not available_filters:
                    hint.update("No discovery filters are available for this connector")
                    return

                hint.update("")
                selected_filters = self.config.selected_filters_for_connector(self.config.university)
                widgets = []
                for group in available_filters:
                    key = str(group["key"])
                    label = _bidi_display(group["label"])
                    options = group.get("options", [])
                    selected_codes = set(selected_filters.get(key, []))

                    selection_list = SelectionList(id=f"filter_list__{key}", classes="filter-selection-list")
                    for item in options:
                        code = str(item["code"])
                        option_label = _bidi_display(item["label"])
                        is_selected = code in selected_codes if selected_codes else True
                        selection_list.add_option((f"{option_label} ({code})", code, is_selected))

                    actions = Horizontal(
                        Button("Select All", id=f"select_all_filter__{key}"),
                        Button("Clear", id=f"clear_filter__{key}"),
                        classes="select-actions",
                    )
                    widgets.append(
                        Collapsible(
                            Label(label, classes="field-label"),
                            selection_list,
                            actions,
                            title=label,
                            collapsed=False,
                            id=f"filter_group__{key}",
                        )
                    )

                if widgets:
                    self._filter_render_generation += 1
                    generation = self._filter_render_generation
                    self.call_after_refresh(
                        lambda widgets=tuple(widgets), generation=generation: self._mount_filter_widgets(
                            container, widgets, generation
                        )
                    )

            def _select_all(self, filter_key: str) -> None:
                sel_list = self.query_one(f"#filter_list__{filter_key}", SelectionList)
                sel_list.select_all()

            def _clear_all(self, filter_key: str) -> None:
                sel_list = self.query_one(f"#filter_list__{filter_key}", SelectionList)
                sel_list.deselect_all()

            def refresh_data(self) -> None:
                self._refresh_records_table()
                self._refresh_reviews_table()
                self._refresh_summary()
                self._populate_filter_lists()
                self._refresh_filter_summary()

            def _start_background(self, command: str, **kwargs) -> None:
                if self.active_thread is not None and self.active_thread.is_alive():
                    self._write_log("A run is already active")
                    return
                if not self._write_config_into_state():
                    return
                self.active_runner = PipelineRunner(
                    self.config,
                    event_sink=self._event_from_runner_thread,
                    secret_store=self.secret_store,
                )
                self.active_thread = threading.Thread(
                    target=self._run_command_thread,
                    args=(command,),
                    kwargs=kwargs,
                    daemon=True,
                )
                self.active_thread.start()
                self._set_running(True)

            def _run_command_thread(self, command: str, **kwargs) -> None:
                assert self.active_runner is not None
                try:
                    if command == "full_run":
                        provider = self._preferred_enrich_provider()
                        self.active_runner.full_run(enrich_provider=provider)
                    elif command == "discover":
                        self.active_runner.discover()
                    elif command == "crawl":
                        self.active_runner.crawl()
                    elif command == "parse":
                        self.active_runner.parse()
                    elif command == "enrich":
                        self.active_runner.enrich(kwargs["provider"])
                    elif command == "review":
                        self.active_runner.review()
                    elif command == "export":
                        self.active_runner.export(kwargs.get("format", "json"))
                except Exception as exc:
                    self.call_from_thread(self._write_log, f"Run failed: {exc}")
                finally:
                    self.call_from_thread(self._set_running, False)
                    self.call_from_thread(self.refresh_data)
                    self.call_from_thread(self._populate_filter_lists)
                    self.call_from_thread(self._refresh_filter_summary)

            def _run_export(self) -> None:
                """Export both JSON and JSONL."""
                self._start_background("export", format="json")
                # Queue JSONL after JSON finishes via a short helper
                def _export_jsonl():
                    if self.active_thread is not None:
                        self.active_thread.join()
                    self.call_from_thread(
                        lambda: self._start_background("export", format="jsonl")
                    )
                threading.Thread(target=_export_jsonl, daemon=True).start()

            def _event_from_runner_thread(self, event: RunEvent) -> None:
                self.call_from_thread(self._handle_runner_event, event)

            def _handle_runner_event(self, event: RunEvent) -> None:
                self.status["stage"] = event.stage
                self.status["message"] = event.message or event.kind
                self.status.update(event.data)
                parts = [event.timestamp, event.kind]
                if event.stage:
                    parts.append(event.stage)
                if event.message:
                    parts.append(_bidi_display(event.message))
                if event.data:
                    parts.append(str(event.data))
                self._write_log(" | ".join(parts))

                # Update progress bar
                bar = self.query_one("#progress", ProgressBar)
                if event.kind == "stage_started":
                    bar.update(total=100, progress=0)
                elif event.kind == "stage_completed":
                    bar.update(total=100, progress=100)
                elif "current" in event.data and "total" in event.data:
                    total = event.data["total"]
                    current = event.data["current"]
                    if total > 0:
                        bar.update(total=total, progress=current)

                self._refresh_summary()

            def _set_running(self, running: bool) -> None:
                run_buttons = (
                    "run_full",
                    "run_discover",
                    "run_crawl",
                    "run_parse",
                    "run_enrich_ollama",
                    "run_enrich_openai",
                    "run_review",
                    "run_export",
                    "save_config",
                    "save_token",
                    "clear_token",
                    "doctor",
                )
                for button_id in run_buttons:
                    self.query_one(f"#{button_id}", Button).disabled = running
                self.query_one("#cancel_run", Button).disabled = not running
                self.status["running"] = running
                self._refresh_summary()

            def _preferred_enrich_provider(self) -> str | None:
                if self.query_one("#ollama_enabled", Checkbox).value:
                    return "ollama"
                if self.query_one("#openai_enabled", Checkbox).value:
                    return "openai"
                return None

            def _save_config(self) -> None:
                if not self._write_config_into_state():
                    return
                saved_path = self.config.save(self.config_path)
                self._write_log(f"Saved config to {saved_path}")
                self._refresh_summary()

            def _save_token(self) -> None:
                token_input = self.query_one("#openai_token", Input)
                token = token_input.value.strip()
                if not token:
                    self._write_log("OpenAI token input is empty")
                    return
                self.secret_store.set("openai", token)
                token_input.value = ""
                self._write_log(f"Saved OpenAI token to {self.secret_store.path}")
                self._refresh_summary()

            def _clear_token(self) -> None:
                self.secret_store.clear("openai")
                self._write_log(f"Cleared OpenAI token from {self.secret_store.path}")
                self._refresh_summary()

            def _run_doctor(self) -> None:
                if not self._write_config_into_state():
                    return
                doctor = PipelineRunner(self.config, secret_store=self.secret_store).doctor()
                self._write_log(str(doctor))
                self.status.update(doctor)
                self._refresh_summary()

            def _write_config_into_state(self) -> bool:
                uni_select = self.query_one("#university", Select)
                if uni_select.value and uni_select.value != Select.BLANK:
                    self.config.university = str(uni_select.value)
                self.config.start_url = self.query_one("#start_url", Input).value.strip()
                allowed = self.query_one("#allowed_domains", Input).value.strip()
                self.config.allowed_domains = [d.strip() for d in allowed.split(",") if d.strip()]
                seed_urls = self.query_one("#seed_result_urls", Input).value.strip()
                self.config.seed_result_urls = [item.strip() for item in seed_urls.split(",") if item.strip()]
                self.config.output_root = self.query_one("#output_root", Input).value.strip() or "data"
                try:
                    self.config.throttle_seconds = float(self.query_one("#throttle_seconds", Input).value.strip() or "0.5")
                    self.config.request_timeout_seconds = float(
                        self.query_one("#request_timeout_seconds", Input).value.strip() or "30"
                    )
                    self.config.personal_page_limit = int(
                        self.query_one("#personal_page_limit", Input).value.strip() or "0"
                    )
                    threshold_value = self.query_one("#confidence_threshold", Input).value.strip()
                    if threshold_value:
                        self.config.review.confidence_threshold = float(threshold_value)
                except ValueError as exc:
                    self._write_log(f"Invalid numeric input: {exc}")
                    return False
                self.config.ollama.enabled = self.query_one("#ollama_enabled", Checkbox).value
                self.config.ollama.base_url = self.query_one("#ollama_base_url", Input).value.strip()
                self.config.ollama.model = self.query_one("#ollama_model", Input).value.strip()
                self.config.openai.enabled = self.query_one("#openai_enabled", Checkbox).value
                self.config.openai.base_url = self.query_one("#openai_base_url", Input).value.strip()
                self.config.openai.model = self.query_one("#openai_model", Input).value.strip()
                self.config.openai.api_key_env = self.query_one("#openai_api_key_env", Input).value.strip() or None
                discovery = self._active_discovery_state()
                if discovery.get("available_filters"):
                    connector_filters: dict[str, list[str]] = {}
                    for group in discovery.get("available_filters", []):
                        key = str(group["key"])
                        list_id = f"#filter_list__{key}"
                        try:
                            selection_list = self.query_one(list_id, SelectionList)
                        except Exception:
                            continue
                        connector_filters[key] = list(selection_list.selected)
                    self.config.set_selected_filters_for_connector(self.config.university, connector_filters)
                return True

            def _refresh_filter_summary(self) -> None:
                connector_filters = self.config.selected_filters_for_connector(self.config.university)
                saved_groups = ", ".join(sorted(connector_filters)) or "none"
                self.query_one("#filters_summary", Static).update(
                    f"Connector: {self.config.university} | Saved filter groups: {saved_groups}"
                )
                self.query_one("#config_filter_summary", Static).update(
                    "Discovery filters are edited in the Filters tab."
                )

            def _mount_filter_widgets(self, container: Vertical, widgets: tuple, generation: int) -> None:
                if generation != self._filter_render_generation:
                    return
                if container.children:
                    return
                container.mount(*widgets)

            def _refresh_summary(self) -> None:
                snapshot = PipelineRunner(self.config, secret_store=self.secret_store).snapshot()
                token, token_source = resolve_provider_api_key(
                    "openai", self.config.openai, secret_store=self.secret_store
                )
                running = self.status.get("running")
                stage = self.status.get("stage") or "-"
                message = self.status.get("message") or "-"
                current_url = self.status.get("url") or self.status.get("source_url") or "-"
                person = _bidi_display(self.status.get("full_name") or "-")

                # Compute progress percentage
                progress_str = ""
                total = self.status.get("total", 0)
                current = self.status.get("current", 0)
                if running and total > 0:
                    pct = min(100, int(current / total * 100))
                    progress_str = f" ({pct}%  {current}/{total})"

                lines = [
                    f"Status: {'RUNNING' if running else 'idle'}  |  Stage: {stage}{progress_str}  |  {_bidi_display(message)}",
                    f"Current: {person}  |  URL: {current_url}",
                    "",
                    f"Records: {snapshot['records_count']}  |  Review: {snapshot['review_queue_count']}  |  HTML: {snapshot['raw_html_count']}  |  PDF: {snapshot['raw_pdf_count']}",
                    f"Crawl Manifest: {snapshot['crawl_manifest_count']}  |  Discovered: {snapshot['discovered_result_links']}",
                    "",
                    f"Ollama: {'ON' if self.config.ollama.enabled else 'off'} ({self.config.ollama.model})  |  OpenAI: {'ON' if self.config.openai.enabled else 'off'} ({self.config.openai.model})",
                    f"OpenAI Token: {'available' if token else 'missing'} ({token_source or 'none'})",
                ]
                self.query_one("#summary", Static).update("\n".join(lines))

            def _configure_tables(self) -> None:
                records = self.query_one("#records", DataTable)
                records.cursor_type = "row"
                records.add_columns("Name", "Rank", "Role", "Email", "Department", "Links")

                reviews = self.query_one("#reviews", DataTable)
                reviews.cursor_type = "row"
                reviews.add_columns("Name", "Confidence", "Flags")

            def _refresh_records_table(self) -> None:
                table = self.query_one("#records", DataTable)
                table.clear(columns=False)
                storage = Storage(self.config.output_path)
                for record in storage.all_records():
                    links = {link.kind: link.kind for link in record.links}
                    link_summary = ", ".join(sorted(links.values()))
                    dept = ""
                    if record.org_affiliations:
                        dept = record.org_affiliations[0].department or ""
                    table.add_row(
                        _bidi_display(record.full_name),
                        _bidi_display(record.current_rank or ""),
                        _bidi_display(record.current_role or ""),
                        record.primary_email or "",
                        _bidi_display(dept),
                        link_summary,
                    )

            def _refresh_reviews_table(self) -> None:
                table = self.query_one("#reviews", DataTable)
                table.clear(columns=False)
                storage = Storage(self.config.output_path)
                review_queue = storage.load_json("review/queue.json", default=[])
                for item in review_queue:
                    flags = ", ".join(flag["reason"] for flag in item.get("review_flags", []))
                    table.add_row(
                        _bidi_display(item.get("full_name", "")),
                        str(item.get("confidence", "")),
                        flags,
                    )

            def _write_log(self, message: str) -> None:
                self.query_one("#log", RichLog).write(message)

        return _OuHarvestTextualApp(*args, **kwargs)
