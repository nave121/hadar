#
# ou-harvest setup & run script (PowerShell)
#
# Usage:
#   .\setup.ps1               # install + doctor check
#   .\setup.ps1 run           # full pipeline run (discover -> export)
#   .\setup.ps1 tui           # launch the TUI
#   .\setup.ps1 <stage>       # run a single stage
#   .\setup.ps1 test          # run tests
#   .\setup.ps1 doctor        # check dependencies and config
#
Set-StrictMode -Version Latest
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
    $candidates = @(
        @{ Command = "py"; Args = @("-3") },
        @{ Command = "python"; Args = @() },
        @{ Command = "python3"; Args = @() }
    )

    foreach ($c in $candidates) {
        $cmd = Get-Command $c.Command -ErrorAction SilentlyContinue
        if ($cmd) {
            try {
                $ver = & $c.Command @($c.Args + @("-c", "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")) 2>$null
                if ($LASTEXITCODE -ne 0) {
                    continue
                }
                $major = [int]($ver.Split('.')[0])
                $minor = [int]($ver.Split('.')[1])
                if (($major -gt 3) -or ($major -eq 3 -and $minor -ge 11)) {
                    return [PSCustomObject]@{
                        Command = $c.Command
                        Args = $c.Args
                    }
                }
            } catch {
                continue
            }
        }
    }

    throw "Python 3.11+ is required but not found."
}

function Get-VenvPythonPath {
    $candidates = @(
        (Join-Path $VENV_DIR "Scripts\python.exe"),
        (Join-Path $VENV_DIR "Scripts\python"),
        (Join-Path $VENV_DIR "bin\python"),
        (Join-Path $VENV_DIR "bin/python")
    )

    foreach ($path in $candidates) {
        if (Test-Path $path) {
            return $path
        }
    }

    return (Join-Path $VENV_DIR "Scripts\python.exe")
}

function Ensure-Venv {
    $venvPython = Get-VenvPythonPath
    if (-not (Test-Path $venvPython)) {
        Write-Bold "Creating virtual environment..."
        $py = Ensure-Python
        & $py.Command @($py.Args + @("-m", "venv", $VENV_DIR))
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to create virtual environment in $VENV_DIR"
        }
        $venvPython = Get-VenvPythonPath
        if (-not (Test-Path $venvPython)) {
            throw "Virtual environment was created, but Python executable was not found."
        }
        Write-Green "Virtual environment created at $VENV_DIR"
    }

    return $venvPython
}

function Invoke-OuHarvest ([string]$py_exe, [string[]]$CliArgs) {
    & $py_exe -m ou_harvest.cli --config $CONFIG @CliArgs
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

function Ensure-Installed ([string]$py_exe) {
    & $py_exe -c "import ou_harvest" 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Bold "Installing ou-harvest with all extras..."
        & $py_exe -m pip install --upgrade pip --quiet
        & $py_exe -m pip install -e ".[tui,playwright,pdf]" --quiet
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to install ou-harvest into $VENV_DIR"
        }
        Write-Green "Package installed."
    }
    return $py_exe
}

function Ensure-Playwright ([string]$py_exe) {
    & $py_exe -c "import playwright" 2>$null
    if ($LASTEXITCODE -ne 0) {
        return
    }

    $playwrightCache = Join-Path ([System.Environment]::GetFolderPath("LocalApplicationData")) "ms-playwright"
    if (-not (Test-Path $playwrightCache)) {
        Write-Bold "Installing Playwright Chromium browser..."
        & $py_exe -m playwright install chromium
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to install Playwright Chromium."
        }
        Write-Green "Playwright Chromium installed."
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

function Get-EnabledProvider {
    if (-not (Test-Path $CONFIG)) {
        return $null
    }

    $configContent = Get-Content $CONFIG -Raw
    if ([regex]::IsMatch($configContent, '(?s)\[ollama\].*?enabled\s*=\s*true')) {
        return "ollama"
    }
    if ([regex]::IsMatch($configContent, '(?s)\[openai\].*?enabled\s*=\s*true')) {
        return "openai"
    }

    return $null
}

function Test-DemographicsEnabled {
    if (-not (Test-Path $CONFIG)) {
        return $false
    }

    $configContent = Get-Content $CONFIG -Raw
    return [regex]::IsMatch($configContent, '(?s)\[demographics\].*?enabled\s*=\s*true')
}

# ---------- commands ----------
function Cmd-Install ([string]$py_exe) {
    $py_exe = Ensure-Installed $py_exe
    Ensure-Playwright $py_exe
    Ensure-Config
    Write-Bold "Running doctor..."
    Invoke-OuHarvest $py_exe @("doctor")
    Write-Green "`nSetup complete. Run '.\setup.ps1 tui' or '.\setup.ps1 run' to start."
}

function Cmd-Doctor ([string]$py_exe) {
    $py_exe = Ensure-Installed $py_exe
    Ensure-Config
    Invoke-OuHarvest $py_exe @("doctor")
}

function Cmd-Test ([string]$py_exe, [string[]]$ExtraArgs = @()) {
    $py_exe = Ensure-Installed $py_exe
    & $py_exe -m pip install pytest --quiet
    Write-Bold "Running tests..."
    & $py_exe -m pytest tests/ -v @ExtraArgs
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

function Cmd-Tui ([string]$py_exe) {
    $py_exe = Ensure-Installed $py_exe
    Ensure-Config
    Invoke-OuHarvest $py_exe @("tui")
}

function Cmd-Run ([string]$py_exe) {
    $py_exe = Ensure-Installed $py_exe
    Ensure-Config
    Write-Bold "Running full pipeline..."
    Write-Host ""

    $steps = @(
        @{ Label = "Discover"; Args = @("discover") },
        @{ Label = "Crawl"; Args = @("crawl") },
        @{ Label = "Parse"; Args = @("parse") }
    )

    if (Test-DemographicsEnabled) {
        $steps += @{ Label = "Demographics"; Args = @("demographics") }
    }

    $provider = Get-EnabledProvider
    if ($null -ne $provider) {
        $steps += @{ Label = "Enrich ($provider)"; Args = @("enrich", "--provider", $provider) }
    }

    $steps += @{ Label = "Review"; Args = @("review", "--json") }
    $steps += @{ Label = "Export"; Args = @("export", "--format", "json") }
    $steps += @{ Label = "Export JSONL"; Args = @("export", "--format", "jsonl") }

    for ($i = 0; $i -lt $steps.Count; $i++) {
        $step = $steps[$i]
        Write-Bold ("[{0}/{1}] {2}" -f ($i + 1), $steps.Count, $step.Label)
        Invoke-OuHarvest $py_exe $step.Args
        Write-Host ""
    }

    Write-Green "Full pipeline complete. Output in data/exports/"
}

function Cmd-Stage ([string]$py_exe, [string]$stage, [string[]]$StageArgs = @()) {
    $py_exe = Ensure-Installed $py_exe
    Ensure-Config

    switch ($stage) {
        { $_ -in "discover","crawl","parse","demographics" } {
            $cliArgs = @($stage) + $StageArgs
            Invoke-OuHarvest $py_exe $cliArgs
        }
        "review" {
            $cliArgs = @($stage) + $StageArgs
            Invoke-OuHarvest $py_exe $cliArgs
        }
        "enrich" {
            if ($StageArgs.Count -eq 0) {
                Write-Red "Usage: .\setup.ps1 enrich --provider <ollama|openai>"
                exit 1
            }
            $cliArgs = @("enrich") + $StageArgs
            Invoke-OuHarvest $py_exe $cliArgs
        }
        "export" {
            if ($StageArgs.Count -gt 0) {
                $cliArgs = @("export") + $StageArgs
                Invoke-OuHarvest $py_exe $cliArgs
            } else {
                Invoke-OuHarvest $py_exe @("export", "--format", "json")
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
if ($null -eq $cmd) { $cmd = "install" }

switch ($cmd) {
    "install" {
        $py_venv = Ensure-Venv
        Cmd-Install $py_venv
    }
    "run" {
        $py_venv = Ensure-Venv
        Cmd-Run $py_venv
    }
    "tui" {
        $py_venv = Ensure-Venv
        Cmd-Tui $py_venv
    }
    "test" {
        $py_venv = Ensure-Venv
        $remainingArgs = @()
        if ($args.Count -gt 1) {
            $remainingArgs = $args[1..($args.Count - 1)]
        }
        Cmd-Test $py_venv $remainingArgs
    }
    "doctor" {
        $py_venv = Ensure-Venv
        Cmd-Doctor $py_venv
    }
    { $_ -in "discover","crawl","parse","demographics","enrich","review","export" } {
        $py_venv = Ensure-Venv
        $stage = $cmd
        $remainingArgs = @()
        if ($args.Count -gt 1) {
            $remainingArgs = $args[1..($args.Count - 1)]
        }
        Cmd-Stage $py_venv $stage $remainingArgs
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
        Write-Host ""
        Write-Host "Stages:"
        Write-Host "  discover, crawl, parse, demographics, enrich, review, export"
    }
    Default {
        Write-Red "Unknown command: $cmd"
        Write-Host "Run '.\setup.ps1 help' for usage."
        exit 1
    }
}
