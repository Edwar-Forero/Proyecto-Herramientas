#!/bin/bash
# Script para arrancar el backend de Estadísticas Vitales Colombia en Bash

cd "$(dirname "$0")/backend" || exit

echo -e "\e[36mIniciando Backend...\e[0m"

if [ ! -d "venv" ]; then
    echo "Creando entorno virtual..."
    python -m venv venv
fi

# Activar entorno virtual y actualizar pip
source venv/Scripts/activate
echo "Actualizando pip..."
python -m pip install --upgrade pip

# Instalar dependencias
echo "Instalando dependencias..."
pip install -r requirements.txt

# Arrancar servidor
echo -e "\e[32mArrancando servidor uvicorn en http://localhost:8000...\e[0m"
python -m uvicorn app.main:app --reload
