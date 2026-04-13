# OU Harvest

Incremental public-profile harvesting pipeline for Israeli university staff directories.

Available as a stage-based CLI or a full-screen Textual TUI with live status, config editing, and record inspection.

Currently supported connectors: Open University of Israel (`openu`), Ben-Gurion University of the Negev (`bgu`), and Technion Faculty of Medicine (`technion_med`).

## Design constraints

- Deterministic scraping first — structured fields are extracted by HTML parsers, not LLMs
- Normalized nested JSON as the canonical output (`PersonRecord`)
- Incremental reruns with content fingerprints — unchanged pages are skipped
- Optional demographics analysis for public profile photos when explicitly enabled and run
- Optional LLM enrichment only for public professional fields that appear in crawled university pages, linked CRIS pages, or public CV files

The default crawl/parse flow only captures public profile content. Demographics inference is an optional, separate stage that runs only when explicitly enabled or invoked.

## Supported data

- Full name, rank/title, name variants
- Role, staff type, department, faculty/unit, office, phones, email
- Directory page, department page, personal page, CRIS link, CV link, ORCID link
- Research interests and public academic links
- Education chronology
- Appointment chronology
- CV-derived publications, awards, academic service, and notable professional links

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[tui,playwright,pdf]
playwright install chromium
cp ou_harvest.toml.example ou_harvest.toml
ou_harvest doctor --config ou_harvest.toml
ou_harvest tui --config ou_harvest.toml
```

## CLI commands

```bash
# Diagnostics
ou_harvest doctor --config ou_harvest.toml

# Pipeline stages (run in order, or use the TUI)
ou_harvest discover --config ou_harvest.toml
ou_harvest crawl --config ou_harvest.toml
ou_harvest parse --config ou_harvest.toml
ou_harvest demographics --config ou_harvest.toml
ou_harvest enrich --config ou_harvest.toml --provider ollama
ou_harvest enrich --config ou_harvest.toml --provider openai
ou_harvest review --config ou_harvest.toml --json
ou_harvest export --config ou_harvest.toml --format json
ou_harvest export --config ou_harvest.toml --format jsonl

# Full-screen TUI
ou_harvest tui --config ou_harvest.toml
```

## TUI

The TUI (`pip install -e .[tui]`) provides:

- Run any pipeline stage or a full run with one button
- Edit runtime config (URLs, throttle, limits, provider settings) and save back to `ou_harvest.toml`
- Live log panel streaming all pipeline events
- Records table and review queue table
- Set or clear a local OpenAI token without putting it in the shared config
- Cancel a running stage mid-flight

Keybindings: `q` quit, `r` refresh data, `c` cancel run.

## Pipeline stages

| Stage | What it does |
|-------|-------------|
| **discover** | Fetches the staff directory landing page, extracts result page links and department links, merges with `seed_result_urls` from config |
| **crawl** | Follows result page links with pagination. Fetches linked personal pages, CVs (PDF), and CRIS pages. All content stored as content-addressed artifacts in `data/raw/` |
| **parse** | Reads stored HTML artifacts, routes through the appropriate parser (results page vs personal page), merges records by `person_id`. PDFs are text-extracted and attached as artifacts |
| **demographics** | Analyzes downloaded profile photos with DeepFace and stores demographic estimates on records that have image artifacts |
| **enrich** | Sends profile text and CV text (chunked at ~10k chars) to an LLM extractor. Extracts education, appointments, publications, awards, academic service, and notable links. Low-confidence results get review flags |
| **review** | Collects records below the confidence threshold or with review flags into `data/review/queue.json` |
| **export** | Writes `data/exports/people.json` or `people.jsonl` |

Stages are idempotent. Running `crawl` without a prior `discover` will trigger discovery automatically. The `demographics` and `enrich` stages can be re-run independently.

## Configuration reference

All settings live in `ou_harvest.toml` (TOML format). See `ou_harvest.toml.example` for a working template.

### Root settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `start_url` | string | `https://www.openu.ac.il/staff/pages/default.aspx` | Discovery landing page |
| `seed_result_urls` | list[string] | `[]` | Additional result page URLs to always include |
| `allowed_domains` | list[string] | `["openu.ac.il", "academic.openu.ac.il"]` | Domain allowlist for the fetcher |
| `request_timeout_seconds` | float | `30` | HTTP request timeout |
| `throttle_seconds` | float | `0.5` | Minimum delay between requests |
| `user_agent` | string | `ou-harvest/0.1` | User-Agent header |
| `output_root` | string | `data` | Root directory for all pipeline output |
| `personal_page_limit` | int | `0` | Max personal pages to fetch globally across the crawl (0 = unlimited) |

### [ollama]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable Ollama as enrichment provider |
| `base_url` | string | `http://localhost:11434` | Ollama API base URL |
| `model` | string | `llama3.1:8b` | Model name |

### [openai]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable OpenAI as enrichment provider |
| `base_url` | string | `https://api.openai.com/v1` | OpenAI-compatible API base URL |
| `model` | string | `gpt-4.1-mini` | Model name |
| `api_key_env` | string or null | `OPENAI_API_KEY` | Environment variable name for the API key |

### [demographics]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Include the demographics stage in `full_run()` |
| `detector_backend` | string | `retinaface` | DeepFace detector backend passed to photo analysis |

### [review]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `confidence_threshold` | float | `0.78` | Records below this threshold are flagged for review |

## Data layout

```
data/
  raw/
    html/          # Downloaded HTML pages (content-addressed by SHA256 prefix)
    pdf/           # Downloaded PDF files (CVs)
    image/         # Downloaded profile photos
    text/          # Extracted text from PDFs
  records/         # Canonical PersonRecord JSON documents (one per person)
  review/          # Review flags and queue.json
  exports/         # people.json and people.jsonl snapshots
  state/
    discovery.json       # Last discovery snapshot
    fingerprints.json    # URL -> SHA256 checksum map for incremental reruns
    crawl_manifest.json  # List of all crawled URLs
```

## LLM enrichment

Enrichment is a two-pass process per record:

1. **Profile text** — source evidence excerpts from directory/personal pages are concatenated (up to 12k chars) and sent to the extractor
2. **CV text** — if a text-extracted CV artifact exists, it's chunked into ~10k-char segments and each chunk is sent separately

Both passes use the same JSON schema (`EXTRACTION_SCHEMA` in `llm.py`) and produce structured education, appointments, publications, awards, academic service, and notable links. Each extracted item carries a confidence score. Items below `review.confidence_threshold` are flagged in `review_flags`.

Providers:
- `ollama` — local inference via the Ollama `/api/generate` endpoint with `format: json`
- `openai` — OpenAI-compatible `/chat/completions` endpoint with `response_format: json_object`

## API key management

The OpenAI API key is resolved in this order:

1. **Environment variable** — the variable named by `openai.api_key_env` (default: `OPENAI_API_KEY`)
2. **User-local secret store** — platform-specific path managed by the TUI:
   - macOS: `~/Library/Application Support/ou-harvest/secrets.json`
   - Linux: `~/.config/ou-harvest/secrets.json`
   - Windows: `%APPDATA%/ou-harvest/secrets.json`

The shared `ou_harvest.toml` never contains the actual token.

## Optional dependencies

Install extras as needed:

| Extra | Packages | Purpose |
|-------|----------|---------|
| `demographics` | deepface>=0.0.93, tf-keras>=2.16, opencv-python>=4.9 | Static image demographic analysis |
| `tui` | textual>=0.52 | Full-screen terminal UI |
| `playwright` | playwright>=1.44 | Browser-rendered page fetching (for JS-heavy pages) |
| `pdf` | pdfplumber>=0.11, pypdf>=4.2 | PDF text extraction from CV files |

Activate Playwright mode by creating a `.use_playwright` sentinel file in the project root.

## PDF text extraction

The `text_extract` module uses a fallback chain:

1. **pdfplumber** (preferred) — layout-aware extraction
2. **pypdf** — fallback if pdfplumber is not installed
3. **pdftotext** — system command fallback (`pdftotext -layout`)

Pages are joined with double newlines. Extracted text is stored as a `text` artifact in `data/raw/text/`.

## Testing

```bash
pytest                          # all tests
pytest tests/test_parsers.py    # single file
pytest tests/test_parsers.py::test_parse_discovery_page_collects_result_and_department_staff_links  # single test
```

Tests use HTML fixtures in `tests/fixtures/` captured from real OU pages. The `pythonpath` is configured to `src` in `pyproject.toml`. Some tests use `unittest.mock` and `monkeypatch` for stubbing HTTP responses and pipeline factories.
