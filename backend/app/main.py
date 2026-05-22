"""Punto de entrada FastAPI — Estadísticas Vitales Colombia."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.database.mongodb import mongodb
from app.routes import (
    analytics_router,
    auth_router,
    dashboard_router,
    mortalidad_router,
    natalidad_router,
    upload_router,
    users_router,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    mongodb.connect()
    yield
    await mongodb.disconnect()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="Estadísticas Vitales Colombia API",
        description="API de Business Intelligence para nacimientos y defunciones (DANE 2020–2024)",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origin_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(auth_router)
    app.include_router(dashboard_router)
    app.include_router(natalidad_router)
    app.include_router(mortalidad_router)
    app.include_router(analytics_router)
    app.include_router(users_router)
    app.include_router(upload_router)

    @app.get("/health")
    async def health():
        return {"status": "ok", "service": "estadisticas-vitales-api"}

    return app


app = create_app()
