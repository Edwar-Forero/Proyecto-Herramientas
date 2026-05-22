<#
.SYNOPSIS
Script para arrancar el backend de Estadísticas Vitales Colombia

.DESCRIPTION
Instala dependencias si es necesario y arranca el servidor uvicorn.
#>

$ErrorActionPreference = "Stop"

Set-Location -Path "$PSScriptRoot\backend"

Write-Host "Iniciando Backend..." -ForegroundColor Cyan

if (-Not (Test-Path "venv")) {
    Write-Host "Creando entorno virtual..."
    python -m venv venv
}

# Activar entorno virtual y actualizar pip
.\venv\Scripts\activate
Write-Host "Actualizando pip..."
python -m pip install --upgrade pip

# Instalar dependencias
Write-Host "Instalando dependencias..."
pip install -r requirements.txt

# Arrancar servidor
Write-Host "Arrancando servidor uvicorn en http://localhost:8000..." -ForegroundColor Green
python -m uvicorn app.main:app --reload
