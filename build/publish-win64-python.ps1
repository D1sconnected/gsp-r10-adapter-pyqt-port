param(
    [switch]$Clean
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$DistRoot = Join-Path $ProjectRoot 'dist'
$BuildRoot = Join-Path $ProjectRoot 'build\pyinstaller'
$SpecPath = Join-Path $ProjectRoot 'build\gsp-r10-python.spec'
$ExePath = Join-Path $DistRoot 'gsp-r10-python.exe'
$SettingsOut = Join-Path $DistRoot 'settings.json'

if ($Clean) {
    if (Test-Path $DistRoot) { Remove-Item $DistRoot -Recurse -Force }
    if (Test-Path $BuildRoot) { Remove-Item $BuildRoot -Recurse -Force }
}

python -m PyInstaller --noconfirm --clean --distpath $DistRoot --workpath $BuildRoot $SpecPath

Copy-Item (Join-Path $ProjectRoot 'settings.json') $SettingsOut -Force

Write-Host ''
Write-Host 'Build complete:'
Write-Host $ExePath
Write-Host "settings.json copied to $SettingsOut"
