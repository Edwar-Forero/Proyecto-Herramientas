#!/bin/bash
# ──────────────────────────────────────────────────────────────────────────────
# dev.sh — Script unificado para arrancar Backend + Frontend en paralelo
# Uso: bash dev.sh
# Pulsa Ctrl+C para detener ambos procesos.
# ──────────────────────────────────────────────────────────────────────────────

set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"
BACKEND_DIR="$ROOT/backend"
FRONTEND_DIR="$ROOT/frontend"

CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

banner() {
    echo -e "${YELLOW}──────────────────────────────────────────────────────────────${NC}"
    echo -e "${YELLOW}  $1${NC}"
    echo -e "${YELLOW}──────────────────────────────────────────────────────────────${NC}"
}

# ──────────────────────────────────────────────────────────────────────────────
# BACKEND — entorno virtual
# ──────────────────────────────────────────────────────────────────────────────
banner "Preparando Backend (FastAPI + uvicorn en :8000)"

if [ ! -d "$BACKEND_DIR/venv" ]; then
    echo -e "${CYAN}[BACKEND] Creando entorno virtual...${NC}"
    python3 -m venv "$BACKEND_DIR/venv"
fi

echo -e "${CYAN}[BACKEND] Instalando/actualizando dependencias...${NC}"
"$BACKEND_DIR/venv/bin/pip" install --quiet --upgrade pip
"$BACKEND_DIR/venv/bin/pip" install --quiet -r "$BACKEND_DIR/requirements.txt"

# ──────────────────────────────────────────────────────────────────────────────
# FRONTEND — node_modules
# ──────────────────────────────────────────────────────────────────────────────
banner "Preparando Frontend (Next.js en :3000)"

if [ ! -d "$FRONTEND_DIR/node_modules" ]; then
    echo -e "${MAGENTA}[FRONTEND] Instalando dependencias npm...${NC}"
    npm --prefix "$FRONTEND_DIR" install
fi

# ──────────────────────────────────────────────────────────────────────────────
# LANZAR AMBOS EN PARALELO
# ──────────────────────────────────────────────────────────────────────────────
banner "Arrancando Backend y Frontend — Ctrl+C para detener"

echo ""
echo -e "  ${CYAN}Backend  → http://localhost:8000${NC}"
echo -e "  ${CYAN}API Docs → http://localhost:8000/docs${NC}"
echo -e "  ${MAGENTA}Frontend → http://localhost:3000${NC}"
echo ""

# Función para matar ambos al salir
cleanup() {
    echo -e "\n${YELLOW}Deteniendo servidores...${NC}"
    kill "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null || true
    wait "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null || true
    echo -e "${GREEN}Servidores detenidos.${NC}"
}
trap cleanup INT TERM

# Arrancar backend
(
    cd "$BACKEND_DIR"
    "$BACKEND_DIR/venv/bin/python" -m uvicorn app.main:app --reload 2>&1 | \
    while IFS= read -r line; do echo -e "${CYAN}[BACKEND]  $line${NC}"; done
) &
BACKEND_PID=$!

# Arrancar frontend
(
    npm --prefix "$FRONTEND_DIR" run dev 2>&1 | \
    while IFS= read -r line; do echo -e "${MAGENTA}[FRONTEND] $line${NC}"; done
) &
FRONTEND_PID=$!

# Esperar a que alguno termine (o Ctrl+C)
wait "$BACKEND_PID" "$FRONTEND_PID"
