<#
.SYNOPSIS
  Creates infra/.env from .env.example and fills in generated secrets.

.DESCRIPTION
  Safe to re-run: values that are already set are never changed. This matters for
  AIRFLOW_FERNET_KEY, because changing it makes already-stored connections unreadable.
  Also creates the dags/, plugins/ and logs/ folders that Airflow bind-mounts.
#>
[CmdletBinding()]
param(
    [string]$InfraDir = (Split-Path -Parent $PSScriptRoot)
)
$ErrorActionPreference = 'Stop'

$example = Join-Path $InfraDir '.env.example'
$envFile = Join-Path $InfraDir '.env'

function New-RandomBytes([int]$Count) {
    $bytes = New-Object byte[] $Count
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    return , $bytes
}

function New-HexSecret([int]$Bytes) {
    return -join ((New-RandomBytes $Bytes) | ForEach-Object { $_.ToString('x2') })
}

# A Fernet key is 32 random bytes, URL-safe base64 encoded.
function New-FernetKey {
    return [Convert]::ToBase64String((New-RandomBytes 32)).Replace('+', '-').Replace('/', '_')
}

$generators = @{
    POSTGRES_PASSWORD       = { New-HexSecret 12 }
    GRAFANA_ADMIN_PASSWORD  = { New-HexSecret 12 }
    AIRFLOW_FERNET_KEY      = { New-FernetKey }
    AIRFLOW_API_SECRET_KEY  = { New-HexSecret 32 }
    AIRFLOW_JWT_SECRET      = { New-HexSecret 32 }
}

if (-not (Test-Path $example)) { throw "Missing $example" }
$source = if (Test-Path $envFile) { $envFile } else { $example }
if ($source -eq $example) { Write-Host "Creating $envFile from .env.example" }
else { Write-Host "$envFile exists; only filling empty values" }

$generated = @()
$lines = Get-Content $source | ForEach-Object {
    if ($_ -match '^([A-Z0-9_]+)=(.*)$' -and $generators.ContainsKey($Matches[1])) {
        $key = $Matches[1]
        $value = $Matches[2].Trim()
        if ($value -eq '' -or $value -eq 'change-me') {
            $generated += $key
            return "$key=$(& $generators[$key])"
        }
    }
    $_
}

# UTF-8 without BOM and LF line endings: a BOM would corrupt the first variable name.
[System.IO.File]::WriteAllText($envFile, (($lines -join "`n") + "`n"), (New-Object System.Text.UTF8Encoding($false)))

foreach ($k in $generated) { Write-Host "  generated $k" }
if (-not $generated) { Write-Host '  nothing to generate' }

$repoRoot = Split-Path -Parent $InfraDir
foreach ($d in 'dags', 'plugins', 'logs') {
    New-Item -ItemType Directory -Force -Path (Join-Path $repoRoot $d) | Out-Null
}
Write-Host 'Done. Next: docker compose up -d --wait   (from the infra/ folder)'
