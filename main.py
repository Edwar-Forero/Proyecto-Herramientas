"""
Punto de entrada en la raíz del proyecto: carga masiva a MongoDB Atlas.

Uso (desde la raíz del repositorio):
    python main.py

Requiere las mismas variables de entorno que describe notebooks/creacionBD.md
(por ejemplo MONGODB_ATLAS_URI).
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.mongo_atlas_loader.main import main

if __name__ == "__main__":
    main()
