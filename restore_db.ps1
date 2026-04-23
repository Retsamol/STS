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

$dumpInfo = Get-Item $dumpPath
$dumpHeaderBytes = Get-Content $dumpPath -Encoding Byte -TotalCount 5
$dumpHeader = [System.Text.Encoding]::ASCII.GetString($dumpHeaderBytes)
$firstTextLine = Get-Content $dumpPath -TotalCount 1 -ErrorAction SilentlyContinue

if ($firstTextLine -like "version https://git-lfs.github.com/spec/v1*") {
    throw @"
Instead of a real dump file, the repository contains a Git LFS pointer.

In the local repository clone, run:
  git lfs install
  git lfs pull

Then check that the dump file is large, not just a few hundred bytes:
  $dumpPath

After that, run restore_db.ps1 again.
"@
}

if ($dumpInfo.Length -lt 1024) {
    throw "The dump file is suspiciously small: $($dumpInfo.Length) bytes. Most likely this is not the real database dump."
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

if ($dumpHeader -eq "PGDMP") {
    docker exec $ContainerName pg_restore `
        -U $User `
        -d $Database `
        --clean `
        --if-exists `
        --no-owner `
        --no-privileges `
        $containerDump
}
else {
    docker exec $ContainerName psql `
        -U $User `
        -d $Database `
        -f $containerDump
}

if ($LASTEXITCODE -ne 0) {
    throw "Database restore failed."
}

Write-Host "Restore finished."
Write-Host "PostgreSQL: postgresql://${User}:${Password}@127.0.0.1:5432/$Database"
