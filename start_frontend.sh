#!/bin/bash
# Script para arrancar el frontend de Estadísticas Vitales Colombia en Bash

cd "$(dirname "$0")/frontend" || exit

echo -e "\e[36mIniciando Frontend...\e[0m"

if [ ! -d "node_modules" ]; then
    echo "Instalando dependencias..."
    npm install
fi

# Arrancar servidor de desarrollo
echo -e "\e[32mArrancando Next.js en http://localhost:3000...\e[0m"
npm run dev
