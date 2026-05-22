from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.database import get_db
from app.schemas.dashboard import KPIsResponse, OverviewResponse
from app.schemas.common import SystemStatus
from app.services.dashboard_service import dashboard_service
from app.utils.dependencies import CurrentUser

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


@router.get("/kpis", response_model=KPIsResponse)
async def get_kpis(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(2020, ge=2020, le=2030),
    year_to: int | None = Query(2024, ge=2020, le=2030),
):
    return await dashboard_service.kpis(db, year_from, year_to)


@router.get("/overview", response_model=OverviewResponse)
async def get_overview(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(2020, ge=2020, le=2030),
    year_to: int | None = Query(2024, ge=2020, le=2030),
):
    return await dashboard_service.overview(db, year_from, year_to)


@router.get("/system-status", response_model=SystemStatus)
async def system_status(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await dashboard_service.system_status(db)
