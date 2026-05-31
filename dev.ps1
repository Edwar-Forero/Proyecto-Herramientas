<#
.SYNOPSIS
    Script unificado — Estadísticas Vitales Colombia
    Lanza Backend (FastAPI :8000) y Frontend (Next.js :3000) en paralelo.

.DESCRIPTION
    Primera vez  : powershell -ExecutionPolicy Bypass -File .\dev.ps1 -Setup
    Desarrollo   : powershell -ExecutionPolicy Bypass -File .\dev.ps1
    O más simple : npm run dev   (desde la raíz del proyecto)

.PARAMETER Setup
    Solo instala/actualiza dependencias sin arrancar los servidores.
#>

param(
    [switch]$Setup
)

$ErrorActionPreference = "Stop"
$Root        = $PSScriptRoot
$BackendDir  = Join-Path $Root "backend"
$FrontendDir = Join-Path $Root "frontend"
$VenvDir     = Join-Path $BackendDir "venv"
$PipExe      = Join-Path $VenvDir "Scripts\pip.exe"
$PythonExe   = Join-Path $VenvDir "Scripts\python.exe"

# ──────────────────────────────────────────────────────────────────────────────
function Write-Banner {
    param([string]$Text, [string]$Color = "Yellow")
    $line = "─" * 64
    Write-Host "`n$line" -ForegroundColor $Color
    Write-Host "  $Text" -ForegroundColor $Color
    Write-Host "$line" -ForegroundColor $Color
}

# ──────────────────────────────────────────────────────────────────────────────
# 1. BACKEND — entorno virtual + dependencias
# ──────────────────────────────────────────────────────────────────────────────
Write-Banner "Backend — preparando entorno Python" "Cyan"

if (-Not (Test-Path $VenvDir)) {
    Write-Host "[BACKEND] Creando entorno virtual..." -ForegroundColor Cyan
    & python -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) { throw "Error creando el venv. ¿Tienes Python 3.10+ instalado?" }
}

Write-Host "[BACKEND] Actualizando pip y dependencias..." -ForegroundColor Cyan
& $PipExe install --quiet --upgrade pip
& $PipExe install --quiet -r (Join-Path $BackendDir "requirements.txt")
Write-Host "[BACKEND] Dependencias listas." -ForegroundColor Green

# ──────────────────────────────────────────────────────────────────────────────
# 2. FRONTEND — node_modules
# ──────────────────────────────────────────────────────────────────────────────
Write-Banner "Frontend — preparando dependencias npm" "Magenta"

if (-Not (Test-Path (Join-Path $FrontendDir "node_modules"))) {
    Write-Host "[FRONTEND] Instalando node_modules..." -ForegroundColor Magenta
    & npm --prefix $FrontendDir install
    if ($LASTEXITCODE -ne 0) { throw "Error instalando dependencias npm del frontend." }
}
Write-Host "[FRONTEND] Dependencias listas." -ForegroundColor Green

# Si solo se quería setup, salir aquí
if ($Setup) {
    Write-Host "`nSetup completado. Ejecuta 'npm run dev' o '.\dev.ps1' para arrancar." -ForegroundColor Yellow
    exit 0
}

# ──────────────────────────────────────────────────────────────────────────────
# 3. LANZAR AMBOS EN PARALELO
# ──────────────────────────────────────────────────────────────────────────────
Write-Banner "Arrancando servidores en paralelo — Ctrl+C para detener" "Yellow"

Write-Host ""
Write-Host "  Backend  → http://localhost:8000      " -ForegroundColor Cyan
Write-Host "  API Docs → http://localhost:8000/docs " -ForegroundColor Cyan
Write-Host "  Frontend → http://localhost:3000      " -ForegroundColor Magenta
Write-Host ""

# Lanzar backend como proceso independiente
$BackendProc = Start-Process -FilePath $PythonExe `
    -ArgumentList "-m uvicorn app.main:app --reload" `
    -WorkingDirectory $BackendDir `
    -PassThru `
    -NoNewWindow

# Lanzar frontend como proceso independiente
$FrontendProc = Start-Process -FilePath "npm" `
    -ArgumentList "--prefix `"$FrontendDir`" run dev" `
    -WorkingDirectory $Root `
    -PassThru `
    -NoNewWindow

Write-Host "[INFO] Backend PID  : $($BackendProc.Id)" -ForegroundColor Cyan
Write-Host "[INFO] Frontend PID : $($FrontendProc.Id)" -ForegroundColor Magenta
Write-Host ""
Write-Host "Pulsa Ctrl+C para detener ambos servidores." -ForegroundColor Yellow

# Esperar, y detener todo al salir
try {
    # Monitorear hasta que alguno muera inesperadamente
    while (-not $BackendProc.HasExited -and -not $FrontendProc.HasExited) {
        Start-Sleep -Seconds 1
    }

    if ($BackendProc.HasExited -and $BackendProc.ExitCode -ne 0) {
        Write-Host "[BACKEND] Terminó con código $($BackendProc.ExitCode)." -ForegroundColor Red
    }
    if ($FrontendProc.HasExited -and $FrontendProc.ExitCode -ne 0) {
        Write-Host "[FRONTEND] Terminó con código $($FrontendProc.ExitCode)." -ForegroundColor Red
    }
}
finally {
    Write-Host "`nDeteniendo servidores..." -ForegroundColor Yellow
    if (-not $BackendProc.HasExited)  { $BackendProc.Kill()  }
    if (-not $FrontendProc.HasExited) { $FrontendProc.Kill() }
    Write-Host "Servidores detenidos." -ForegroundColor Green
}
