#
# ou-harvest setup & run script (PowerShell)
#
# Usage:
#   .\setup.ps1               # install + doctor check
#   .\setup.ps1 run           # full pipeline run (discover -> export)
#   .\setup.ps1 tui           # launch the TUI
#   .\setup.ps1 <stage>       # run a single stage (discover, crawl, parse, enrich, review, export)
#   .\setup.ps1 test          # run tests
#   .\setup.ps1 doctor        # check dependencies and config
#
$ErrorActionPreference = "Stop"

$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $SCRIPT_DIR

$VENV_DIR = ".venv"
$CONFIG = "ou_harvest.toml"
$CONFIG_EXAMPLE = "ou_harvest.toml.example"

# ---------- colors ----------
function Write-Green ([string]$msg) { Write-Host $msg -ForegroundColor Green }
function Write-Red ([string]$msg) { Write-Host $msg -ForegroundColor Red }
function Write-Bold ([string]$msg) { Write-Host $msg -ForegroundColor Cyan }

# ---------- helpers ----------
function Ensure-Python {
    $py = $null
    $candidates = @("python", "python3")
    foreach ($c in $candidates) {
        $cmd = Get-Command $c -ErrorAction SilentlyContinue
        if ($cmd) {
            $ver = & $c -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
            $major = [int]($ver.Split('.')[0])
            $minor = [int]($ver.Split('.')[1])
            if ($major -ge 3 -and $minor -ge 11) {
                $py = $c
                break
            }
        }
    }
    if (-not $py) {
        Write-Red "Python 3.11+ is required but not found."
        exit 1
    }
    return $py
}

function Ensure-Venv {
    if (-not (Test-Path $VENV_DIR)) {
        Write-Bold "Creating virtual environment..."
        $py = Ensure-Python
        & $py -m venv $VENV_DSS
        $py_venv = Join-Path $VENV_DIR "Scripts\python.exe"
        Write-Green "Virtual environment created at $VENV_DIR"
    }
    # Return the path to the python executable in venv
    return Join-Path $VENV_DIR "Scripts\python.exe"
}

function Ensure-Installed ([string]$py_exe) {
    if (-not ( & $py_exe -c "import ou_harvest" 2>$null )) {
        Write-Bold "Installing ou-harvest with all extras..."
        & $py_exe -m pip install --upgrade pip --quiet
        & $py_exe -m pip install -e ".[tui,playwright,pdf]" --quiet
        Write-Green "Package installed."
    }
    return $py_exe
}

function Ensure-Playwright ([string]$py_exe) {
    if (& $py_exe -c "import playwright" 2>$format) {
        # On Windows, we check user cache or local app data
        $playwright_cache = [System.Environment]::GetFolderPath("LocalApplicationData") + "\ms-playwright"
        if (-not (Test-Path $playwright_cache)) {
            Write-Bold "Installing Playwright Chromium browser..."
            & $py_exe -m playwright install chromium
            Write-Green "Playwright Chromium installed."
        }
    }
}

function Ensure-Config {
    if (-not (Test-Path $CONFIG)) {
        if (Test-Path $CONFIG_EXAMPLE) {
            Copy-Item $CONFIG_EXAMPLE $CONFIG
            Write-Green "Created $CONFIG from $CONFIG_EXAMPLE"
        } else {
            Write-Red "No $CONFIG_EXAMPLE found. Cannot create config."
            exit 1
        }
    }
}

# ---------- commands ----------
function Cmd-Install ([string]$py_exe) {
    $py_exe = Ensure-Installed $py_exe
    Ensure-Playwright $py_exe
    Ensure-Config
    Write-Bold "Running doctor..."
    & $py_exe -m ou_harvest --config $CONFIG doctor
    Write-Green "`nSetup complete. Run '.\setup.ps1 tui' or '.\setup ps1 run' to start."
}

function Cmd-Doctor ([string]$py_exe) {
    $py_exe = Ensure-Installed $py_exe
    Ensure-Config
    & $py_exe -m ou_harvest --config $CONFIG doctor
}

function Cmd-Test ([string]$py_exe) {
    $py_exe = Ensure-Installed $py_exe
    & $py_exe -m pip install pytest --quiet
    Write-Bold "Running tests..."
    & $py_exe -m pytest tests/ -v $args
}

function Cmd-Tui ([string]$py_exe) {
    $py_exe = Ensure-Installed $py_exe
    Ensure-Config
    & $py_exe -m ou_harvest --config $CONFIG tui
}

function Cmd-Run ([string]$py_exe) {
    $py_exe = Ensure-Installed $py_exe
    Ensure-Config
    Write-Bold "Running full pipeline..."
    Write-Host ""

    Write-Bold "[1/6] Discover"
    & $py_exe -m ou_harvest --config $CONFIG discover
    Write-Host ""

    Write-HTML "[2/6] Crawl"
    & $py_exe -m ou_harvest --config $CONFIG crawl
    Write-Host ""

    Write-Bold "[3/6] Parse"
    & $py_exe -m ou_harvest --config $CONFIG parse
    Write-Host ""

    # Determine enrich provider from config
    $provider = ""
    $config_content = Get-Content $CONFIG -Raw
    if ($config_content -match '\[ollama\].*?enabled = true') {
        $provider = "ollama"
    } elseif ($config_content -match '\[openai\].*?enabled = true') {
        $provider = "openai"
    }

    if ($provider -ne "") {
        Write-Bold "[4/6] Enrich ($provider)"
        & $py_exe -m ou_harvest --config $CONFIG enrich --provider $provider
    } else {
        Write-Bold "[4/6] Enrich (skipped - no provider enabled)"
    }
    Write-Host ""

    Write-Bold "[5/6] Review"
    & $py_exe -m ou_harvest --config $CONFIG review --json
    Write-Host ""

    Write-Bold "[6/6] Export"
    & $py_exe -m ou_harvest --config $CONFIG export --format json
    & $py_exe -m ou_harvest --config $CONFIG export --format jsonl
    Write-Host ""

    Write-Green "Full pipeline complete. Output in data/exports/"
}

function Cmd-Stage ([string]$py_exe, [string]$stage) {
    $py_exe = Ensure-Installed $py_exe
    Ensure-Config
    $args_list = $args
    
    switch ($stage) {
        { $_ -in "discover","crawl","parse","review" } {
            & $py_exe -m ou_harvest --config $CONFIG $stage $args_list
        }
        "enrich" {
            if ($args_list.Count -eq 0) {
                Write-Red "Usage: .\setup.ps1 enrich --provider <ollama|openai>"
                exit 1
            }
            & $py_exe -m ou_harvest --config $CONFIG enrich $args_list
        }
        "export" {
            if ($args_list.Count -gt 0 -and $args_list[0] -eq "--format") {
                & $py_exe -m ou_harvest --config $CONFIG export $args_list
            } else {
                & $py_exe -m ou_harvest --config $CONFIG export --format json
            }
        }
        Default {
            Write-Red "Unknown stage: $stage"
            exit 1
        }
    }
}

# ---------- main ----------
$cmd = $args[0]
if ($null -eq $cmd) { $cmd = "help" }

$py_venv = Ensure-Venv

switch ($cmd) {
    "install" { Cmd-Install $py_venv }
    "run"      { Cmd-Run $py_venv }
    "tui"      { Cmd-Tui $py_venv }
    "test"     { Cmd-Test $py_venv $args[1..($args.Count-1)] }
    "doctor"   { Cmd-Doctor $py_venv }
    { $_ -in "discover","crawl","parse","enrich","review","export" } {
        $stage = $cmd
        $remaining_args = $args[1..($args.Count-1)]
        Cmd-Stage $py_venv $stage $remaining_args
    }
    "help" {
        Write-Bold "ou-harvest setup & run script (PowerShell)"
        Write-Host ""
        Write-Host "Usage:"
        Write-Host "  .\setup.ps1               Install dependencies and run doctor"
        Write-Host "  .\setup.ps1 run           Full pipeline (discover -> export)"
        Write-Host "  .\setup.ps1 tui           Launch the TUI"
        Write-Host "  .\setup.ps1 <stage>       Run a single stage"
        Write-Host "  .\setup.ps1 test          Run tests"
        Write-Host "  .\setup.ps1 doctor        Check config and dependencies"
        Write-Host "  .\setup.ps1 help          Show this help"
    }
    Default {
        Write-Red "Unknown command: $cmd"
        Write-Host "Run '.\setup.ps1 help' for usage."
    }
}
