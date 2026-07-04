param(
  [int]$Pg = 18,
  [string]$Version = "",
  [string]$PgConfig = "",
  [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
Set-Location $RootDir

if (-not $Version) {
  $cargo = Get-Content -Path "Cargo.toml" -Raw
  if ($cargo -notmatch '(?m)^\[workspace\.package\]\s*(?:\n[^\[]*)?^version\s*=\s*"([^"]+)"') {
    throw "workspace.package.version not found in Cargo.toml"
  }
  $Version = $Matches[1]
}

$Distro = "windows"
$Arch = "amd64"
$Formats = "zip"
$ExtensionCrate = "pg_koldstore"
$ExtensionSqlName = "koldstore"
$PgrxVersion = if ($env:PGRX_VERSION) { $env:PGRX_VERSION } else { "0.19.1" }

function Get-ArtifactBaseName {
  param(
    [string]$Version,
    [int]$Pg,
    [string]$Distro,
    [string]$Arch,
    [string]$Ext
  )
  return "pg_koldstore-v$Version-pg$Pg-$Distro-$Arch.$Ext"
}

function Ensure-CargoPgrx {
  $installed = $false
  try {
    $versionLine = (cargo pgrx --version 2>$null)
    if ($versionLine -match "cargo-pgrx $PgrxVersion") {
      $installed = $true
    }
  } catch {
    $installed = $false
  }
  if (-not $installed) {
    cargo install cargo-pgrx --version $PgrxVersion --locked
  }
}

if (-not $PgConfig) {
  Ensure-CargoPgrx
  cargo pgrx init "--pg$Pg" download
  $PgConfig = (cargo pgrx info pg-config $Pg).Trim()
}

if (-not (Test-Path $PgConfig)) {
  throw "pg_config not found: $PgConfig"
}

if (-not $SkipBuild) {
  Ensure-CargoPgrx
  cargo pgrx init "--pg$Pg" $PgConfig
  cargo pgrx package `
    -p $ExtensionCrate `
    --no-default-features `
    --features "pg$Pg" `
    --pg-config $PgConfig
}

$packageRoot = Join-Path $RootDir "target/release/$ExtensionSqlName-pg$Pg"
if (-not (Test-Path $packageRoot)) {
  throw "expected pgrx package directory $packageRoot"
}

$innerName = Get-ArtifactBaseName -Version $Version -Pg $Pg -Distro $Distro -Arch $Arch -Ext "tree"
$distDir = Join-Path $RootDir "dist/$Version"
$stageParent = New-Item -ItemType Directory -Path (Join-Path $env:TEMP ([System.Guid]::NewGuid().ToString())) -Force
$stageDir = Join-Path $stageParent.FullName $innerName
New-Item -ItemType Directory -Force -Path $stageDir | Out-Null
Copy-Item "$packageRoot/*" $stageDir -Recurse

@'
param([string]$PgConfig = "pg_config")
$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$LibDir = & $PgConfig --pkglibdir
$ShareDir = Join-Path (& $PgConfig --sharedir) "extension"
$Dll = Get-ChildItem -Path $ScriptDir -Recurse -Filter "koldstore.dll" | Select-Object -First 1
if (-not $Dll) { throw "koldstore.dll not found" }
Copy-Item $Dll.FullName (Join-Path $LibDir $Dll.Name) -Force
Copy-Item (Get-ChildItem -Path $ScriptDir -Recurse -Filter "koldstore.control" | Select-Object -First 1).FullName $ShareDir -Force
Get-ChildItem -Path $ScriptDir -Recurse -Filter "koldstore--*.sql" | ForEach-Object {
  Copy-Item $_.FullName $ShareDir -Force
}
Write-Host "Installed koldstore. Run: CREATE EXTENSION koldstore;"
'@ | Set-Content -Path (Join-Path $stageDir "install.ps1") -Encoding UTF8

$zipName = Get-ArtifactBaseName -Version $Version -Pg $Pg -Distro $Distro -Arch $Arch -Ext "zip"
New-Item -ItemType Directory -Force -Path $distDir | Out-Null
$zipPath = Join-Path $distDir $zipName
if (Test-Path $zipPath) {
  Remove-Item $zipPath -Force
}
Compress-Archive -Path $stageDir -DestinationPath $zipPath -Force
Remove-Item $stageParent.FullName -Recurse -Force
Write-Host "created $zipPath"
