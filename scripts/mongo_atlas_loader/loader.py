"""
Servicios de carga, indices y usuarios para MongoDB Atlas.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.server_api import ServerApi

from .config import DataSource, MongoAtlasSettings


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadResult:
    collection_name: str
    parquet_path: Path
    total_rows: int
    inserted_rows: int


def create_client(settings: MongoAtlasSettings) -> MongoClient:
    """Crea cliente MongoDB Atlas con Server API v1."""
    return MongoClient(settings.uri, server_api=ServerApi("1"))


def ping_client(client: MongoClient) -> None:
    """Valida conectividad al cluster."""
    client.admin.command("ping")
    LOGGER.info("Ping exitoso con MongoDB Atlas.")


def ensure_collections(db: Database, names: list[str]) -> None:
    """Crea colecciones si aun no existen."""
    existing = set(db.list_collection_names())
    for name in names:
        if name not in existing:
            db.create_collection(name)
            LOGGER.info("Coleccion creada: %s", name)
        else:
            LOGGER.info("Coleccion existente: %s", name)


def _to_documents(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """Convierte DataFrame a lista de documentos MongoDB."""
    if frame.empty:
        return []
    cleaned = frame.where(pd.notnull(frame), None)
    return cleaned.to_dict(orient="records")


def load_parquet_in_batches(
    collection: Collection,
    parquet_path: Path,
    batch_size: int,
) -> LoadResult:
    """Carga un parquet en lotes usando pandas.read_parquet + insert_many."""
    if not parquet_path.exists():
        raise FileNotFoundError(f"No existe archivo parquet: {parquet_path}")

    frame = pd.read_parquet(parquet_path)
    total_rows = len(frame)
    inserted_rows = 0

    LOGGER.info(
        "Iniciando carga de %s en %s (%s filas).",
        parquet_path.name,
        collection.name,
        f"{total_rows:,}",
    )

    if total_rows == 0:
        return LoadResult(collection.name, parquet_path, 0, 0)

    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        chunk = frame.iloc[start:end]
        docs = _to_documents(chunk)
        if not docs:
            continue
        collection.insert_many(docs, ordered=False)
        inserted_rows += len(docs)
        LOGGER.info(
            "Coleccion %s: lote %s - %s insertado (%s/%s).",
            collection.name,
            start + 1,
            end,
            f"{inserted_rows:,}",
            f"{total_rows:,}",
        )

    return LoadResult(collection.name, parquet_path, total_rows, inserted_rows)


def create_indexes(db: Database) -> None:
    """Crea indices operativos para consultas BI."""
    fields = ["ano", "mes", "cod_dpto", "cod_munic", "sexo", "tipo_evento"]
    target_collections = [
        "nacimientos",
        "defunciones_fetales",
        "defunciones_no_fetales",
    ]

    for collection_name in target_collections:
        collection = db[collection_name]
        for field in fields:
            index_name = f"idx_{field}"
            collection.create_index([(field, ASCENDING)], name=index_name, sparse=True)
        LOGGER.info("Indices creados/verificados para %s.", collection_name)


def _hash_password(password: str) -> str:
    """Hashea password con PBKDF2-HMAC SHA256."""
    iterations = 200_000
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${base64.b64encode(salt).decode()}${base64.b64encode(digest).decode()}"


def seed_users(db: Database) -> None:
    """Inserta/actualiza usuarios iniciales con upsert por username."""
    users_collection = db["usuarios"]

    base_users = [
        {"username": "admin", "password": "admin123", "rol": "admin"},
        {"username": "analista", "password": "analista123", "rol": "analista"},
        {"username": "consulta", "password": "consulta123", "rol": "consulta"},
    ]

    operations = []
    for user in base_users:
        operations.append(
            UpdateOne(
                {"username": user["username"]},
                {
                    "$set": {
                        "username": user["username"],
                        "rol": user["rol"],
                        "password_hash": _hash_password(user["password"]),
                        "activo": True,
                    }
                },
                upsert=True,
            )
        )

    users_collection.bulk_write(operations, ordered=False)
    users_collection.create_index("username", unique=True, name="ux_username")
    LOGGER.info("Usuarios iniciales insertados/actualizados.")


def run_full_load(db: Database, sources: list[DataSource], batch_size: int) -> list[LoadResult]:
    """Ejecuta carga completa de fuentes parquet."""
    results: list[LoadResult] = []
    for source in sources:
        collection = db[source.collection_name]
        result = load_parquet_in_batches(collection, source.parquet_path, batch_size=batch_size)
        results.append(result)
    return results

