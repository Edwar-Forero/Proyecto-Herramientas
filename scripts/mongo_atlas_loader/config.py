"""
Configuracion central para carga masiva a MongoDB Atlas.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DATA_DIR = ROOT_DIR / "data" / "processed"
SCRIPT_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class MongoAtlasSettings:
    """Parametros de conexion y ejecucion del proceso de carga."""

    uri: str
    database_name: str = "estadisticas_vitales"
    batch_size: int = 10_000


@dataclass(frozen=True)
class DataSource:
    """Relacion entre archivo parquet y coleccion destino."""

    parquet_path: Path
    collection_name: str


def _load_env_file() -> None:
    """Carga variables desde .env si no existen en el entorno actual."""
    candidate_files = [
        ROOT_DIR / ".env",
        SCRIPT_DIR / ".env",
    ]

    for env_path in candidate_files:
        if not env_path.exists():
            continue

        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def get_settings() -> MongoAtlasSettings:
    """Construye configuracion desde variables de entorno."""
    _load_env_file()

    uri = os.getenv("MONGODB_ATLAS_URI", "").strip()
    if not uri:
        raise ValueError(
            "No se encontro MONGODB_ATLAS_URI. "
            "Definela en variables de entorno antes de ejecutar."
        )

    database_name = os.getenv("MONGODB_DATABASE", "estadisticas_vitales").strip()
    batch_size_raw = os.getenv("MONGODB_BATCH_SIZE", "10000").strip()

    try:
        batch_size = int(batch_size_raw)
    except ValueError as exc:
        raise ValueError("MONGODB_BATCH_SIZE debe ser un entero positivo.") from exc

    if batch_size <= 0:
        raise ValueError("MONGODB_BATCH_SIZE debe ser mayor a 0.")

    return MongoAtlasSettings(
        uri=uri,
        database_name=database_name,
        batch_size=batch_size,
    )


def get_sources() -> list[DataSource]:
    """Fuentes de datos procesadas del proyecto."""
    return [
        DataSource(PROCESSED_DATA_DIR / "nac.parquet", "nacimientos"),
        DataSource(PROCESSED_DATA_DIR / "fetal.parquet", "defunciones_fetales"),
        DataSource(PROCESSED_DATA_DIR / "nofetal.parquet", "defunciones_no_fetales"),
    ]

