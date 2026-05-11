param(
    [switch]$ResetVolume,
    [switch]$RestoreDump,
    [string]$DumpPath = ""
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$ComposeFile = Join-Path $Root "infra\postgres\docker-compose.yml"

function Invoke-DockerStep {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args,
        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    & docker @Args
    if ($LASTEXITCODE -ne 0) {
        throw "$Label failed with exit code $LASTEXITCODE"
    }
}

function Invoke-ComposeStep {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args,
        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    & docker compose -f $ComposeFile @Args
    if ($LASTEXITCODE -ne 0) {
        throw "$Label failed with exit code $LASTEXITCODE"
    }
}

function Wait-Postgres {
    for ($i = 0; $i -lt 60; $i++) {
        & docker compose -f $ComposeFile exec -T topo-postgres pg_isready -U postgres -d topo | Out-Null
        if ($LASTEXITCODE -eq 0) {
            return
        }
        Start-Sleep -Seconds 2
    }
    throw "PostgreSQL did not become ready in time"
}

function Apply-SqlFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RelativePath
    )

    $SqlPath = Join-Path $Root $RelativePath
    if (-not (Test-Path $SqlPath)) {
        throw "SQL file not found: $SqlPath"
    }

    Write-Host "Applying $RelativePath"
    Get-Content -Raw -Encoding UTF8 $SqlPath | docker compose -f $ComposeFile exec -T topo-postgres psql -v ON_ERROR_STOP=1 -U postgres -d topo | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to apply $RelativePath"
    }
}

function Resolve-DumpPath {
    if ($DumpPath -ne "") {
        $ExplicitPath = Join-Path $Root $DumpPath
        if (-not (Test-Path $ExplicitPath)) {
            throw "Dump file not found: $ExplicitPath"
        }
        return $ExplicitPath
    }

    $ExportsDir = Join-Path $Root "db\exports"
    $LatestDump = Get-ChildItem $ExportsDir -Filter "*.dump" -File | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($null -eq $LatestDump) {
        throw "No .dump file found in $ExportsDir"
    }
    return $LatestDump.FullName
}

function Resolve-SchemaPath {
    $Schema = Get-ChildItem (Join-Path $Root "db") -Filter "topo_results_schema_*.sql" -File |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if ($null -eq $Schema) {
        throw "No topo schema SQL file found in db"
    }
    return $Schema.FullName
}

if (-not (Test-Path $ComposeFile)) {
    throw "Docker Compose file not found: $ComposeFile"
}

if ($ResetVolume) {
    Write-Host "Resetting PostgreSQL Docker volume"
    Invoke-ComposeStep -Args @("down", "-v") -Label "docker compose down -v"
}

Write-Host "Starting PostgreSQL"
Invoke-ComposeStep -Args @("up", "-d") -Label "docker compose up"
Wait-Postgres

if ($RestoreDump) {
    $DumpFullPath = Resolve-DumpPath
    $DumpItem = Get-Item $DumpFullPath
    if ($DumpItem.Length -lt 1000000) {
        throw "Dump file is too small. It is probably a Git LFS pointer, not the real dump: $DumpFullPath"
    }

    Write-Host "Restoring dump $($DumpItem.Name)"
    Invoke-DockerStep -Args @("cp", $DumpFullPath, "topo-postgres:/tmp/topo_results.dump") -Label "docker cp dump"
    Invoke-ComposeStep -Args @(
        "exec",
        "-T",
        "topo-postgres",
        "pg_restore",
        "--clean",
        "--if-exists",
        "--no-owner",
        "--no-privileges",
        "-U",
        "postgres",
        "-d",
        "topo",
        "/tmp/topo_results.dump"
    ) -Label "pg_restore"
} else {
    $SchemaFullPath = Resolve-SchemaPath
    $RelativeSchemaPath = $SchemaFullPath.Substring($Root.Length + 1)
    Apply-SqlFile $RelativeSchemaPath
}

Write-Host "Done"
