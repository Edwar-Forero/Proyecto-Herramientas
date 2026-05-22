from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.database import get_db
from app.schemas.dashboard import (
    NatalidadByDepartmentResponse,
    NatalidadByGenderResponse,
    NatalidadByYearResponse,
)
from app.schemas.common import CategoryPoint
from app.services.natalidad_service import natalidad_service
from app.utils.dependencies import CurrentUser

router = APIRouter(prefix="/natalidad", tags=["Natalidad"])


@router.get("/by-year", response_model=NatalidadByYearResponse)
async def by_year(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(None, ge=2020, le=2030),
    year_to: int | None = Query(None, ge=2020, le=2030),
    department: str | None = None,
):
    data = await natalidad_service.by_year(db, year_from, year_to, department)
    return NatalidadByYearResponse(data=data)


@router.get("/by-department", response_model=NatalidadByDepartmentResponse)
async def by_department(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    limit: int = Query(10, ge=1, le=33),
    year_from: int | None = Query(None, ge=2020, le=2030),
    year_to: int | None = Query(None, ge=2020, le=2030),
):
    data = await natalidad_service.by_department(db, limit, year_from, year_to)
    return NatalidadByDepartmentResponse(data=data)


@router.get("/by-gender", response_model=NatalidadByGenderResponse)
async def by_gender(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(None, ge=2020, le=2030),
    year_to: int | None = Query(None, ge=2020, le=2030),
):
    data = await natalidad_service.by_gender(db, year_from, year_to)
    return NatalidadByGenderResponse(data=data)


@router.get("/by-education", response_model=list[CategoryPoint])
async def by_education(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(None, ge=2020, le=2030),
    year_to: int | None = Query(None, ge=2020, le=2030),
):
    return await natalidad_service.by_education(db, year_from, year_to)


@router.get("/by-health-regime", response_model=list[CategoryPoint])
async def by_health_regime(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(None, ge=2020, le=2030),
    year_to: int | None = Query(None, ge=2020, le=2030),
):
    return await natalidad_service.by_health_regime(db, year_from, year_to)


@router.get("/by-maternal-age", response_model=list[CategoryPoint])
async def by_maternal_age(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(None, ge=2020, le=2030),
    year_to: int | None = Query(None, ge=2020, le=2030),
):
    return await natalidad_service.by_maternal_age(db, year_from, year_to)
