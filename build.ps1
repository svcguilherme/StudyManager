# Compila o StudyManager.exe e instala em C:\myapps\studymanager (ou -Target).
# Uso: .\build.ps1   |   .\build.ps1 -Target D:\apps\studymanager
[CmdletBinding()]
param(
    [string]$Target = 'C:\myapps\studymanager'
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$python = Join-Path $root '.venv\Scripts\python.exe'

& $python -m PyInstaller --noconfirm --clean --windowed --name StudyManager `
    --distpath (Join-Path $root 'build\dist') --workpath (Join-Path $root 'build\work') --specpath (Join-Path $root 'build') `
    --add-data "$root\studymanager\templates;studymanager\templates" `
    --collect-submodules uvicorn `
    (Join-Path $root 'packaging\app.py')
if ($LASTEXITCODE -ne 0) { throw 'PyInstaller falhou.' }

# Para o app antes de sobrescrever o .exe.
Get-Process StudyManager -ErrorAction SilentlyContinue | Stop-Process -Force

New-Item -ItemType Directory -Force -Path $Target | Out-Null
$internal = Join-Path $Target '_internal'
if (Test-Path $internal) { Remove-Item -Recurse -Force $internal }
Copy-Item -Recurse -Force (Join-Path $root 'build\dist\StudyManager\*') $Target
Copy-Item -Force (Join-Path $root 'docker-compose.yml') $Target
# O .env do destino é preservado; só é criado na primeira instalação.
if (-not (Test-Path (Join-Path $Target '.env'))) {
    $envSource = if (Test-Path (Join-Path $root '.env')) { '.env' } else { '.env.example' }
    Copy-Item (Join-Path $root $envSource) (Join-Path $Target '.env')
}

Write-Host "StudyManager instalado em $Target" -ForegroundColor Green
