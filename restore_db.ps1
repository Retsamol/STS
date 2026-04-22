param(
    [string]$ComposeFile = "infra/postgres/docker-compose.yml",
    [string]$DumpFile = "db/exports/inventory_v2_topo_2026-04-22_22-29-34.dump",
    [string]$ContainerName = "topo-postgres",
    [string]$Database = "topo",
    [string]$User = "postgres",
    [string]$Password = "postgres"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$composePath = Join-Path $root $ComposeFile
$dumpPath = Join-Path $root $DumpFile

if (-not (Test-Path $composePath)) {
    throw "Compose file not found: $composePath"
}

if (-not (Test-Path $dumpPath)) {
    throw "Dump file not found: $dumpPath"
}

docker compose -f $composePath up -d
if ($LASTEXITCODE -ne 0) {
    throw "Failed to start PostgreSQL container."
}

$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    docker exec $ContainerName pg_isready -U $User -d $Database | Out-Null
    if ($LASTEXITCODE -eq 0) {
        $ready = $true
        break
    }
    Start-Sleep -Seconds 2
}

if (-not $ready) {
    throw "PostgreSQL did not become ready in time."
}

$containerDump = "/tmp/" + [System.IO.Path]::GetFileName($dumpPath)

docker cp $dumpPath "${ContainerName}:$containerDump"
if ($LASTEXITCODE -ne 0) {
    throw "Failed to copy dump into container."
}

docker exec $ContainerName pg_restore `
    -U $User `
    -d $Database `
    --clean `
    --if-exists `
    --no-owner `
    --no-privileges `
    $containerDump

if ($LASTEXITCODE -ne 0) {
    throw "pg_restore failed."
}

Write-Host "Restore finished."
Write-Host "PostgreSQL: postgresql://${User}:${Password}@127.0.0.1:5432/$Database"
