"""
Entry-point para crear BD y cargar parquet en MongoDB Atlas.

Uso recomendado desde la raíz del proyecto:
    python main.py

Alternativa:
    python scripts/mongo_atlas_loader/main.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.mongo_atlas_loader.config import get_settings, get_sources
from scripts.mongo_atlas_loader.loader import (
    create_client,
    create_indexes,
    ensure_collections,
    ping_client,
    run_full_load,
    seed_users,
)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def main() -> None:
    setup_logging()
    logger = logging.getLogger("mongo_atlas_loader")

    settings = get_settings()
    sources = get_sources()

    logger.info("Iniciando proceso de carga Atlas.")
    client = create_client(settings)

    try:
        ping_client(client)
        db = client[settings.database_name]

        collection_names = [
            "nacimientos",
            "defunciones_fetales",
            "defunciones_no_fetales",
            "usuarios",
        ]
        ensure_collections(db, collection_names)

        load_results = run_full_load(db, sources, batch_size=settings.batch_size)
        create_indexes(db)
        seed_users(db)

        logger.info("Resumen final:")
        for result in load_results:
            logger.info(
                "- %s (%s): %s/%s filas insertadas",
                result.collection_name,
                result.parquet_path.name,
                f"{result.inserted_rows:,}",
                f"{result.total_rows:,}",
            )
        logger.info("Proceso completado correctamente.")
    finally:
        client.close()


if __name__ == "__main__":
    main()

