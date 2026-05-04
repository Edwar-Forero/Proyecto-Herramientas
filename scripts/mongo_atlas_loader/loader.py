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


# =========================
# CONEXIÓN
# =========================
def create_client(settings: MongoAtlasSettings) -> MongoClient:
    return MongoClient(settings.uri, server_api=ServerApi("1"))


def ping_client(client: MongoClient) -> None:
    client.admin.command("ping")
    LOGGER.info(" Ping exitoso con MongoDB Atlas.")


def ensure_collections(db: Database, names: list[str]) -> None:
    existing = set(db.list_collection_names())
    for name in names:
        if name not in existing:
            db.create_collection(name)
            LOGGER.info("Coleccion creada: %s", name)
        else:
            LOGGER.info("Coleccion existente: %s", name)


def _to_documents(frame: pd.DataFrame) -> list[dict]:
    if frame.empty:
        return []

    frame = frame.copy()
    frame.columns = frame.columns.str.lower().str.strip()

    cleaned = frame.where(pd.notnull(frame), None)
    cleaned = cleaned.astype(object)

    return cleaned.to_dict(orient="records")


def load_parquet_in_batches(
    collection: Collection,
    parquet_path: Path,
    batch_size: int,
) -> LoadResult:

    if not parquet_path.exists():
        raise FileNotFoundError(f"No existe archivo parquet: {parquet_path}")

    frame = pd.read_parquet(parquet_path)
    total_rows = len(frame)
    inserted_rows = 0

    LOGGER.info(
        "Cargando %s → %s (%s filas)",
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

        collection.insert_many(
            docs,
            ordered=False,
            bypass_document_validation=True
        )

        inserted_rows += len(docs)

        LOGGER.info(
            "%s: %s - %s (%s/%s)",
            collection.name,
            start + 1,
            end,
            f"{inserted_rows:,}",
            f"{total_rows:,}",
        )

    return LoadResult(collection.name, parquet_path, total_rows, inserted_rows)


def create_indexes(db: Database) -> None:
    collections = [
        "nacimientos",
        "defunciones_fetales",
        "defunciones_no_fetales",
    ]

    for name in collections:
        col = db[name]

        # índices simples
        col.create_index("cod_dpto")
        col.create_index("cod_munic")
        col.create_index("ano")
        col.create_index("mes")

        # índices compuestos (clave)
        col.create_index(
            [("cod_dpto", ASCENDING), ("cod_munic", ASCENDING)],
            name="idx_geo"
        )

        col.create_index(
            [("ano", ASCENDING), ("mes", ASCENDING)],
            name="idx_tiempo"
        )

        LOGGER.info("Indices creados en %s", name)


def _hash_password(password: str) -> str:
    iterations = 200_000
    salt = secrets.token_bytes(16)

    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations
    )

    return (
        f"pbkdf2_sha256${iterations}$"
        f"{base64.b64encode(salt).decode()}$"
        f"{base64.b64encode(digest).decode()}"
    )


def seed_users(db: Database) -> None:
    users = db["usuarios"]

    base_users = [
        {"username": "admin", "password": "admin123", "rol": "admin"},
        {"username": "analista", "password": "analista123", "rol": "analista"},
        {"username": "consulta", "password": "consulta123", "rol": "consulta"},
    ]

    ops = []
    for u in base_users:
        ops.append(
            UpdateOne(
                {"username": u["username"]},
                {
                    "$set": {
                        "username": u["username"],
                        "rol": u["rol"],
                        "password_hash": _hash_password(u["password"]),
                        "activo": True,
                    }
                },
                upsert=True,
            )
        )

    users.bulk_write(ops, ordered=False)
    users.create_index("username", unique=True)

    LOGGER.info("Usuarios creados")


def run_full_load(
    db: Database,
    sources: list[DataSource],
    batch_size: int
) -> list[LoadResult]:

    results: list[LoadResult] = []

    for source in sources:
        col = db[source.collection_name]

        result = load_parquet_in_batches(
            col,
            source.parquet_path,
            batch_size=batch_size
        )

        results.append(result)

    return results
