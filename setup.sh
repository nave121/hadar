#!/usr/bin/env bash
#
# ou-harvest setup & run script
#
# Usage:
#   ./setup.sh              # install + doctor check
#   ./setup.sh run           # full pipeline run (discover -> export)
#   ./setup.sh tui           # launch the TUI
#   ./setup.sh <stage>       # run a single stage (discover, crawl, parse, enrich, review, export)
#   ./setup.sh test          # run tests
#   ./setup.sh doctor        # check dependencies and config
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR=".venv"
CONFIG="ou_harvest.toml"
CONFIG_EXAMPLE="ou_harvest.toml.example"

# ---------- colors ----------
red()   { printf '\033[0;31m%s\033[0m\n' "$*"; }
green() { printf '\033[0;32m%s\033[0m\n' "$*"; }
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }

# ---------- helpers ----------
ensure_python() {
    local py=""
    for candidate in python3.13 python3.12 python3.11 python3; do
        if command -v "$candidate" &>/dev/null; then
            local ver
            ver="$("$candidate" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
            local major minor
            major="${ver%%.*}"
            minor="${ver##*.}"
            if [[ "$major" -ge 3 && "$minor" -ge 11 ]]; then
                py="$candidate"
                break
            fi
        fi
    done
    if [[ -z "$py" ]]; then
        red "Python 3.11+ is required but not found."
        exit 1
    fi
    echo "$py"
}

ensure_venv() {
    if [[ ! -d "$VENV_DIR" ]]; then
        bold "Creating virtual environment..."
        local py
        py="$(ensure_python)"
        "$py" -m venv "$VENV_DIR"
        green "Virtual environment created at $VENV_DIR"
    fi
    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
}

ensure_installed() {
    ensure_venv
    if ! python -c "import ou_harvest" &>/dev/null; then
        bold "Installing ou-harvest with all extras..."
        pip install --upgrade pip --quiet
        pip install -e ".[tui,playwright,pdf]" --quiet
        green "Package installed."
    fi
}

ensure_playwright() {
    if python -c "import playwright" &>/dev/null; then
        if [[ ! -d "$HOME/Library/Caches/ms-playwright" && ! -d "$HOME/.cache/ms-playwright" ]]; then
            bold "Installing Playwright Chromium browser..."
            playwright install chromium
            green "Playwright Chromium installed."
        fi
    fi
}

ensure_config() {
    if [[ ! -f "$CONFIG" ]]; then
        if [[ -f "$CONFIG_EXAMPLE" ]]; then
            cp "$CONFIG_EXAMPLE" "$CONFIG"
            green "Created $CONFIG from $CONFIG_EXAMPLE"
        else
            red "No $CONFIG_EXAMPLE found. Cannot create config."
            exit 1
        fi
    fi
}

# ---------- commands ----------
cmd_install() {
    ensure_installed
    ensure_playwright
    ensure_config
    bold "Running doctor..."
    ou_harvest --config "$CONFIG" doctor
    echo ""
    green "Setup complete. Run './setup.sh tui' or './setup.sh run' to start."
}

cmd_doctor() {
    ensure_installed
    ensure_config
    ou_harvest --config "$CONFIG" doctor
}

cmd_test() {
    ensure_installed
    pip install pytest --quiet 2>/dev/null
    bold "Running tests..."
    python -m pytest tests/ -v ${@+"$@"}
}

cmd_tui() {
    ensure_installed
    ensure_config
    ou_harvest --config "$CONFIG" tui
}

cmd_run() {
    ensure_installed
    ensure_config
    bold "Running full pipeline..."
    echo ""

    bold "[1/6] Discover"
    ou_harvest --config "$CONFIG" discover
    echo ""

    bold "[2/6] Crawl"
    ou_harvest --config "$CONFIG" crawl
    echo ""

    bold "[3/6] Parse"
    ou_harvest --config "$CONFIG" parse
    echo ""

    # Determine enrich provider from config
    local provider=""
    if grep -q 'enabled = true' <(sed -n '/^\[ollama\]/,/^\[/p' "$CONFIG" 2>/dev/null); then
        provider="ollama"
    elif grep -q 'enabled = true' <(sed -n '/^\[openai\]/,/^\[/p' "$CONFIG" 2>/dev/null); then
        provider="openai"
    fi

    if [[ -n "$provider" ]]; then
        bold "[4/6] Enrich ($provider)"
        ou_harvest --config "$CONFIG" enrich --provider "$provider"
    else
        bold "[4/6] Enrich (skipped - no provider enabled)"
    fi
    echo ""

    bold "[5/6] Review"
    ou_harvest --config "$CONFIG" review --json
    echo ""

    bold "[6/6] Export"
    ou_harvest --config "$CONFIG" export --format json
    ou_harvest --config "$CONFIG" export --format jsonl
    echo ""

    green "Full pipeline complete. Output in data/exports/"
}

cmd_stage() {
    ensure_installed
    ensure_config
    local stage="$1"
    shift
    case "$stage" in
        discover|crawl|parse|review)
            ou_harvest --config "$CONFIG" "$stage" "$@"
            ;;
        enrich)
            if [[ $# -eq 0 ]]; then
                red "Usage: ./setup.sh enrich --provider <ollama|openai>"
                exit 1
            fi
            ou_harvest --config "$CONFIG" enrich "$@"
            ;;
        export)
            local fmt="${1:---format}"
            if [[ "$fmt" == "--format" ]]; then
                ou_harvest --config "$CONFIG" export "$@"
            else
                ou_harvest --config "$CONFIG" export --format json
            fi
            ;;
        *)
            red "Unknown stage: $stage"
            echo "Valid stages: discover, crawl, parse, enrich, review, export"
            exit 1
            ;;
    esac
}

# ---------- main ----------
cmd="${1:-}"
shift 2>/dev/null || true

case "$cmd" in
    ""|install)
        cmd_install
        ;;
    run)
        cmd_run
        ;;
    tui)
        cmd_tui
        ;;
    test)
        cmd_test "$@"
        ;;
    doctor)
        cmd_doctor
        ;;
    discover|crawl|parse|enrich|review|export)
        cmd_stage "$cmd" "$@"
        ;;
    -h|--help|help)
        bold "ou-harvest setup & run script"
        echo ""
        echo "Usage:"
        echo "  ./setup.sh              Install dependencies and run doctor"
        echo "  ./setup.sh run           Full pipeline (discover -> export)"
        echo "  ./setup.sh tui           Launch the TUI"
        echo "  ./setup.sh <stage>       Run a single stage"
        echo "  ./setup.sh test          Run tests"
        echo "  ./setup.sh doctor        Check config and dependencies"
        echo "  ./setup.sh help          Show this help"
        echo ""
        echo "Stages: discover, crawl, parse, enrich, review, export"
        ;;
    *)
        red "Unknown command: $cmd"
        echo "Run './setup.sh help' for usage."
        exit 1
        ;;
esac
