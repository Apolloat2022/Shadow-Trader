<#
.SYNOPSIS
    Builds the Shadow Trader Lambda Layer by pip-installing requirements.
    Run this script from the project root before `terraform apply`.

.EXAMPLE
    .\build_layer.ps1
#>

[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$ProjectRoot  = $PSScriptRoot
$RequirementsFile = Join-Path $ProjectRoot "requirements.txt"
$OutputDir    = Join-Path $ProjectRoot "infra" ".build" "layer" "python"

Write-Host "Shadow Trader — Building Lambda Layer" -ForegroundColor Cyan
Write-Host "Requirements : $RequirementsFile"
Write-Host "Output dir   : $OutputDir"

# ── Validate requirements.txt exists ─────────────────────────────────────────
if (-not (Test-Path $RequirementsFile)) {
    Write-Error "requirements.txt not found at: $RequirementsFile"
    exit 1
}

# ── Clean previous build ──────────────────────────────────────────────────────
if (Test-Path $OutputDir) {
    Write-Host "Cleaning previous build..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force $OutputDir
}
New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

# ── Pip install into the layer directory ─────────────────────────────────────
Write-Host "Installing packages..." -ForegroundColor Green
pip install `
    --requirement $RequirementsFile `
    --target $OutputDir `
    --platform manylinux2014_x86_64 `
    --implementation cp `
    --python-version 3.12 `
    --only-binary=:all: `
    --upgrade

if ($LASTEXITCODE -ne 0) {
    Write-Error "pip install failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "Layer build complete: $OutputDir" -ForegroundColor Green
Write-Host "Run 'terraform apply' from the infra/ directory." -ForegroundColor Cyan
