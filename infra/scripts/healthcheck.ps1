param(
  [string]$ComposeFile = "infra/docker/docker-compose.yml",
  [string]$EnvFile = "infra/docker/.env",
  [int]$TimeoutSec = 300
)

function Get-ComposeArgs {
  param(
    [string]$ComposeFilePath,
    [string]$EnvFilePath
  )

  $args = @("compose", "-f", $ComposeFilePath)
  if (Test-Path $EnvFilePath) {
    $args += @("--env-file", $EnvFilePath)
  }

  return $args
}

function Read-EnvFile {
  param([string]$Path)

  $values = @{}
  if (-not (Test-Path $Path)) {
    return $values
  }

  foreach ($line in Get-Content $Path) {
    $trimmed = $line.Trim()
    if ($trimmed.Length -eq 0 -or $trimmed.StartsWith("#")) {
      continue
    }

    $parts = $trimmed.Split("=", 2)
    if ($parts.Length -ne 2) {
      continue
    }

    $key = $parts[0].Trim()
    $value = $parts[1].Trim()
    if ($key.Length -gt 0) {
      $values[$key] = $value
    }
  }

  return $values
}

if (-not (Test-Path $ComposeFile)) {
  Write-Error "Compose file not found: $ComposeFile"
  exit 1
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
  Write-Error "Docker CLI not found. Install Docker Desktop and try again."
  exit 1
}

$composeArgs = Get-ComposeArgs -ComposeFilePath $ComposeFile -EnvFilePath $EnvFile
$containerIds = docker @composeArgs ps -q
if (-not $containerIds) {
  Write-Error "No containers found. Run docker compose up -d first."
  exit 1
}

$deadline = (Get-Date).AddSeconds($TimeoutSec)
$pending = $true
while ($pending -and (Get-Date) -lt $deadline) {
  $pending = $false
  $containerIds = docker @composeArgs ps -q

  foreach ($id in $containerIds) {
    $inspect = docker inspect $id | ConvertFrom-Json
    $state = $inspect[0].State

    if ($state.Status -ne "running") {
      $pending = $true
      continue
    }

    if ($null -ne $state.Health -and $state.Health.Status -ne "healthy") {
      $pending = $true
      continue
    }
  }

  if ($pending) {
    Start-Sleep -Seconds 5
  }
}

if ($pending) {
  Write-Error "Container health checks did not pass within ${TimeoutSec}s."
  exit 1
}

$apiPort = $env:API_PORT
if (-not $apiPort) {
  $envValues = Read-EnvFile -Path $EnvFile
  if ($envValues.ContainsKey("API_PORT")) {
    $apiPort = $envValues["API_PORT"]
  }
}

if (-not $apiPort) {
  $apiPort = "8000"
}

$apiUrl = "http://localhost:$apiPort/health"
try {
  Invoke-WebRequest -Uri $apiUrl -UseBasicParsing -TimeoutSec 5 | Out-Null
  Write-Host "API health check passed: $apiUrl"
} catch {
  Write-Error "API health check failed: $apiUrl"
  exit 1
}

Write-Host "All health checks passed."
