<#
.SYNOPSIS
Script para arrancar el frontend de Estadísticas Vitales Colombia

.DESCRIPTION
Instala dependencias y arranca el servidor Next.js en modo desarrollo.
#>

$ErrorActionPreference = "Stop"

Set-Location -Path "$PSScriptRoot\frontend"

Write-Host "Iniciando Frontend..." -ForegroundColor Cyan

if (-Not (Test-Path "node_modules")) {
    Write-Host "Instalando dependencias..."
    npm install
}

# Arrancar servidor de desarrollo
Write-Host "Arrancando Next.js en http://localhost:3000..." -ForegroundColor Green
npm run dev
