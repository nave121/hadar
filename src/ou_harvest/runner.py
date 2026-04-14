from __future__ import annotations

import functools
import importlib.util
from pathlib import Path
import threading
from typing import Any, Callable

from .config import AppConfig
from .events import EventSink, RunCancelled, RunEvent
from .pipeline import OuHarvestPipeline
from .secrets import SecretStore, resolve_provider_api_key
from .storage import Storage


PipelineFactory = Callable[..., OuHarvestPipeline]


class PipelineRunner:
    def __init__(
        self,
        config: AppConfig,
        *,
        pipeline_factory: PipelineFactory = OuHarvestPipeline,
        event_sink: EventSink | None = None,
        secret_store: SecretStore | None = None,
    ):
        self.config = config
        self.pipeline_factory = pipeline_factory
        self.event_sink = event_sink
        self.secret_store = secret_store or SecretStore()
        self._cancel_event = threading.Event()

    def cancel(self) -> None:
        self._cancel_event.set()
        self._emit("log", stage="runner", message="Cancellation requested")

    def clear_cancel(self) -> None:
        self._cancel_event.clear()

    def discover(self):
        self.clear_cancel()
        return self._run_stage("discover", lambda pipeline: pipeline.discover())

    def crawl(self):
        self.clear_cancel()
        return self._run_stage("crawl", lambda pipeline: pipeline.crawl())

    def parse(self):
        self.clear_cancel()
        return self._run_stage("parse", lambda pipeline: pipeline.parse())

    def analyze_demographics(self):
        self.clear_cancel()
        if not self.config.demographics.enabled:
            self._emit("log", stage="demographics", message="Demographics stage is disabled in config; skipping")
            return []
        return self._run_stage("demographics", lambda pipeline: pipeline.analyze_demographics())

    def enrich(self, provider_name: str):
        self.clear_cancel()
        return self._run_stage("enrich", lambda pipeline: pipeline.enrich(provider_name), provider=provider_name)

    def review(self):
        self.clear_cancel()
        return self._run_stage("review", lambda pipeline: pipeline.review())

    def export(self, fmt: str):
        self.clear_cancel()
        return self._run_stage("export", lambda pipeline: pipeline.export(fmt), format=fmt)

    def full_run(self, *, enrich_provider: str | None = None) -> dict[str, Any]:
        self.clear_cancel()
        results: dict[str, Any] = {}
        results["discover"] = self._run_stage("discover", lambda pipeline: pipeline.discover())
        if self._cancel_event.is_set():
            raise RunCancelled("Run cancelled after discover")
        results["crawl"] = self._run_stage("crawl", lambda pipeline: pipeline.crawl())
        if self._cancel_event.is_set():
            raise RunCancelled("Run cancelled after crawl")
        results["parse"] = self._run_stage("parse", lambda pipeline: pipeline.parse())
        if self._cancel_event.is_set():
            raise RunCancelled("Run cancelled after parse")
        if self.config.demographics.enabled:
            results["demographics"] = self._run_stage("demographics", lambda pipeline: pipeline.analyze_demographics())
            if self._cancel_event.is_set():
                raise RunCancelled("Run cancelled after demographics")
        if enrich_provider:
            results["enrich"] = self._run_stage(
                "enrich", lambda pipeline: pipeline.enrich(enrich_provider), provider=enrich_provider
            )
            if self._cancel_event.is_set():
                raise RunCancelled("Run cancelled after enrich")
        results["review"] = self._run_stage("review", lambda pipeline: pipeline.review())
        if self._cancel_event.is_set():
            raise RunCancelled("Run cancelled after review")
        results["export_json"] = self._run_stage("export", lambda pipeline: pipeline.export("json"), format="json")
        return results

    def doctor(self) -> dict[str, Any]:
        openai_token, openai_source = resolve_provider_api_key(
            "openai", self.config.openai, secret_store=self.secret_store
        )
        report = {
            "config_path": str(self.config.source_path or Path("ou_harvest.toml")),
            "output_root": str(self.config.output_path),
            "records_count": len(self.storage.all_records()) if self.storage.records.exists() else 0,
            "optional_dependencies": {
                "textual": _has_module("textual"),
                "playwright": _has_module("playwright"),
                "pdfplumber": _has_module("pdfplumber"),
                "pypdf": _has_module("pypdf"),
            },
            "providers": {
                "ollama": {
                    "enabled": self.config.ollama.enabled,
                    "base_url": self.config.ollama.base_url,
                    "model": self.config.ollama.model,
                },
                "openai": {
                    "enabled": self.config.openai.enabled,
                    "base_url": self.config.openai.base_url,
                    "model": self.config.openai.model,
                    "api_key_env": self.config.openai.api_key_env,
                    "api_key_available": bool(openai_token),
                    "api_key_source": openai_source,
                    "secret_store_path": str(self.secret_store.path),
                },
            },
        }
        return report

    def snapshot(self) -> dict[str, Any]:
        crawl_manifest = self.storage.load_json("state/crawl_manifest.json", default={"urls": []})
        discovery = self.storage.load_json("state/discovery.json", default={})
        review_queue = self.storage.load_json("review/queue.json", default=[])
        crawl_requests = crawl_manifest.get("requests")
        crawl_manifest_count = len(crawl_requests) if isinstance(crawl_requests, list) else len(crawl_manifest.get("urls", []))
        return {
            "records_count": len(self.storage.all_records()),
            "raw_html_count": len(list(self.storage.raw_html.glob("*.html"))),
            "raw_json_count": len(list(self.storage.raw_json.glob("*.json"))),
            "raw_pdf_count": len(list(self.storage.raw_pdf.glob("*.pdf"))),
            "raw_image_count": len(list(self.storage.raw_image.glob("*.*"))),
            "review_queue_count": len(review_queue),
            "crawl_manifest_count": crawl_manifest_count,
            "discovered_result_links": len(discovery.get("result_links", [])),
            "exports": sorted(str(path) for path in self.storage.exports.glob("*")),
        }

    @functools.cached_property
    def storage(self) -> Storage:
        return Storage(self.config.output_path)

    def _run_stage(self, stage: str, action: Callable[[OuHarvestPipeline], Any], **data) -> Any:
        self._emit("stage_started", stage=stage, message=f"Starting {stage}", **data)
        pipeline = self.pipeline_factory(
            self.config,
            event_sink=self._emit_from_pipeline,
            should_cancel=self._cancel_event.is_set,
            secret_store=self.secret_store,
        )
        try:
            result = action(pipeline)
        except RunCancelled as exc:
            self._emit("stage_cancelled", stage=stage, message=str(exc), **data)
            raise
        except Exception as exc:
            self._emit("stage_failed", stage=stage, message=str(exc), error_type=type(exc).__name__, **data)
            raise
        summary = self._summarize_result(stage, result)
        summary.update(self.snapshot())
        self._emit("stage_completed", stage=stage, message=f"Completed {stage}", **summary)
        return result

    def _summarize_result(self, stage: str, result: Any) -> dict[str, Any]:
        if stage == "discover":
            return {
                "result_links": len(result.result_links),
                "department_staff_links": len(result.department_staff_links),
            }
        if stage == "crawl":
            return {"crawled_count": len(result)}
        if stage in {"parse", "demographics", "enrich"}:
            return {"record_count": len(result)}
        if stage == "review":
            return {"review_queue_count": len(result)}
        if stage == "export":
            return {"export_path": str(result)}
        return {}

    def _emit_from_pipeline(self, event: RunEvent) -> None:
        self._emit(event.kind, stage=event.stage, message=event.message, **event.data)

    def _emit(self, kind: str, *, stage: str | None, message: str | None = None, **data) -> None:
        if self.event_sink is None:
            return
        self.event_sink(RunEvent(kind=kind, stage=stage, message=message, data=data))


def _has_module(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None
