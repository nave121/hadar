# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

ou-harvest is a stage-based pipeline for harvesting public academic staff profiles from the Open University of Israel directory. It scrapes, parses, and optionally enriches records with LLM extraction (Ollama or OpenAI). It has both a CLI and a Textual-based TUI. Python 3.11+, Pydantic v2, BeautifulSoup4.

## Commands

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -e .[tui,playwright,pdf]
playwright install chromium
cp ou_harvest.toml.example ou_harvest.toml

# Run tests
pytest
pytest tests/test_parsers.py                    # single file
pytest tests/test_parsers.py::test_parse_discovery_page_collects_result_and_department_staff_links  # single test

# CLI stages
ou_harvest discover --config ou_harvest.toml
ou_harvest crawl --config ou_harvest.toml
ou_harvest parse --config ou_harvest.toml
ou_harvest enrich --config ou_harvest.toml --provider ollama
ou_harvest enrich --config ou_harvest.toml --provider openai
ou_harvest review --config ou_harvest.toml --json
ou_harvest export --config ou_harvest.toml --format json
ou_harvest doctor --config ou_harvest.toml
ou_harvest tui --config ou_harvest.toml
```

## Architecture

The pipeline runs six ordered stages: **discover -> crawl -> parse -> enrich -> review -> export**. Each stage reads from and writes to `data/` (configurable via `output_root`).

### Key modules (all under `src/ou_harvest/`)

- **pipeline.py** (`OuHarvestPipeline`) — Core pipeline implementing all six stages. Scraping, parsing, enrichment, and export logic lives here. Caches a reverse checksum map for artifact-to-URL lookups during parse.
- **runner.py** (`PipelineRunner`) — Wraps the pipeline with event emission, cancellation support (via `threading.Event`), and stage orchestration. Used by both CLI and TUI. `storage` is a `cached_property`.
- **models.py** — Pydantic v2 models. `PersonRecord` is the central data type with `merge()` for combining records from multiple sources and `create_id()` for deterministic ID generation (SHA1 of email or name). `dedupe_models()` and `_dedupe_as_dicts()` handle list-field deduplication.
- **parsers.py** — BeautifulSoup HTML parsers for three page types: discovery page, results page (two layouts: table rows via `tr.zebra-generic` and container-based), and personal pages. Handles Hebrew text, RTL content, and OU-specific HTML patterns.
- **storage.py** (`Storage`) — Content-addressed artifact storage and record persistence. Artifacts named by SHA256 prefix. Fingerprints are cached in memory and flushed to disk via `flush_fingerprints()`.
- **llm.py** — LLM extraction via `OllamaExtractor` or `OpenAIExtractor`. Both use the same JSON schema (`EXTRACTION_SCHEMA`) and prompt structure. Returns `{}` on malformed JSON responses instead of crashing.
- **config.py** (`AppConfig`) — TOML-based config with Pydantic validation. `ProviderConfig` has a `field_validator` that coerces empty strings to `None` for `api_key_env`.
- **secrets.py** (`SecretStore`) — User-local secret storage (outside repo) for API keys. Platform-aware paths (macOS: `~/Library/Application Support/ou-harvest/secrets.json`). `resolve_provider_api_key()` checks env var first, then secret store.
- **http.py** — `BaseFetcher` base class with domain allowlist (`_check_domain`) and throttling (`_throttle`). `RequestsFetcher` (default, uses `requests.Session`) and `PlaywrightFetcher` (activated by `.use_playwright` sentinel) both inherit from it. `guess_fetcher()` factory selects the implementation.
- **text_extract.py** — PDF text extraction with fallback chain: pdfplumber -> pypdf -> system `pdftotext` command.
- **tui.py** — Textual full-screen app. Runs pipeline stages in background threads with live event streaming. `_write_config_into_state()` validates numeric inputs and returns `bool` to guard callers.
- **events.py** — `RunEvent` dataclass and `EventSink` callback type for pipeline progress reporting. `RunCancelled` exception for stage cancellation.

### Data flow

1. **discover** — Fetches the staff directory landing page, extracts result page links and department links, merges with `seed_result_urls` from config.
2. **crawl** — Follows result page links (with pagination), fetches linked personal pages, CVs, and CRIS pages. All content stored as content-addressed artifacts in `data/raw/`. `personal_page_limit` is enforced globally across the entire crawl. Fingerprints are flushed to disk at stage end.
3. **parse** — Reads stored HTML artifacts, routes through the appropriate parser (results vs personal page), merges records by `person_id`. PDF CVs are text-extracted via `text_extract.extract_pdf_text()` and attached as artifacts. Uses a cached reverse checksum map for artifact-to-URL lookups.
4. **enrich** — Sends profile text and CV text (chunked at ~10k chars) to an LLM extractor. Results merged into records with per-field confidence scores. Low-confidence extractions get `ReviewFlag`s.
5. **review** — Collects records below the confidence threshold into a review queue.
6. **export** — Writes `data/exports/people.json` or `people.jsonl`.

### Record identity and merging

`PersonRecord.create_id()` generates a deterministic 16-char hex ID from email (preferred) or name. `PersonRecord.merge()` combines two records with deduplication across all list fields using tuple-based keys via `_dedupe_as_dicts()`, which produces plain dicts for consistent `model_validate()` input.

### Content fingerprinting

`Storage` maintains an in-memory fingerprint cache (URL -> SHA256 checksum). `update_fingerprint()` writes to the cache only; `flush_fingerprints()` persists to `data/state/fingerprints.json`. Pipeline stages call `flush_fingerprints()` at their boundaries.

## Testing

Tests use HTML fixtures in `tests/fixtures/` captured from real OU page structures. The `pythonpath` is configured to `src` in `pyproject.toml`. Tests use pytest with `tmp_path`, `monkeypatch`, and `unittest.mock` for stubbing HTTP and pipeline factories.

Test files: `test_parsers.py`, `test_storage.py`, `test_secrets_and_runner.py`, `test_cv_enrichment.py`, `test_http.py`, `test_llm.py`.

## Config

Runtime config lives in `ou_harvest.toml` (TOML format, not committed — listed in `.gitignore` via `data/`). See `ou_harvest.toml.example` for the full schema. The actual OpenAI API key is never stored in the config file — it comes from an env var or the platform-specific secret store. When `api_key_env` is `None`, the TOML serializes it as `""` which is coerced back to `None` on load by a Pydantic field validator.
