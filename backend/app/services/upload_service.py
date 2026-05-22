"""Carga de datasets — solo administrador."""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.models import DEFUNCIONES_FETALES, DEFUNCIONES_NO_FETALES, NACIMIENTOS

COLLECTION_MAP = {
    "nacimientos": NACIMIENTOS,
    "nac": NACIMIENTOS,
    "fetal": DEFUNCIONES_FETALES,
    "defunciones_fetales": DEFUNCIONES_FETALES,
    "nofetal": DEFUNCIONES_NO_FETALES,
    "defunciones_no_fetales": DEFUNCIONES_NO_FETALES,
}

BATCH_SIZE = 5000


class UploadService:
    async def upload_parquet(
        self,
        db: AsyncIOMotorDatabase,
        file_bytes: bytes,
        collection_key: str,
        replace: bool = False,
    ) -> dict:
        collection_name = COLLECTION_MAP.get(collection_key.lower())
        if not collection_name:
            raise ValueError(
                f"Colección inválida. Use: {', '.join(sorted(set(COLLECTION_MAP.keys())))}"
            )

        frame = pd.read_parquet(io.BytesIO(file_bytes))
        frame.columns = frame.columns.str.lower().str.strip()
        docs = frame.where(pd.notnull(frame), None).astype(object).to_dict(orient="records")

        col = db[collection_name]
        if replace:
            await col.delete_many({})

        inserted = 0
        for start in range(0, len(docs), BATCH_SIZE):
            batch = docs[start : start + BATCH_SIZE]
            if batch:
                await col.insert_many(batch, ordered=False)
                inserted += len(batch)

        return {
            "collection": collection_name,
            "inserted": inserted,
            "total_rows": len(docs),
            "replaced": replace,
        }


upload_service = UploadService()
