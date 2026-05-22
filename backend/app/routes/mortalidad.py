from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.database import get_db
from app.schemas.dashboard import MortalidadByRegionResponse, MortalidadByYearResponse
from app.schemas.common import CategoryPoint
from app.services.mortalidad_service import mortalidad_service
from app.utils.dependencies import CurrentUser

router = APIRouter(prefix="/mortalidad", tags=["Mortalidad"])


@router.get("/by-year", response_model=MortalidadByYearResponse)
async def by_year(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(None, ge=2020, le=2030),
    year_to: int | None = Query(None, ge=2020, le=2030),
):
    fetal, no_fetal = await mortalidad_service.by_year(db, year_from, year_to)
    return MortalidadByYearResponse(fetal=fetal, no_fetal=no_fetal)


@router.get("/by-region", response_model=MortalidadByRegionResponse)
async def by_region(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    limit: int = Query(10, ge=1, le=33),
    year_from: int | None = Query(None, ge=2020, le=2030),
    year_to: int | None = Query(None, ge=2020, le=2030),
):
    fetal, no_fetal = await mortalidad_service.by_region(db, limit, year_from, year_to)
    return MortalidadByRegionResponse(fetal=fetal, no_fetal=no_fetal)


@router.get("/causes-fetal", response_model=list[CategoryPoint])
async def causes_fetal(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(None, ge=2020, le=2030),
    year_to: int | None = Query(None, ge=2020, le=2030),
):
    return await mortalidad_service.causes_fetal(db, year_from, year_to)


@router.get("/age-groups", response_model=list[CategoryPoint])
async def age_groups(
    current_user: CurrentUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(None, ge=2020, le=2030),
    year_to: int | None = Query(None, ge=2020, le=2030),
):
    return await mortalidad_service.age_groups_nofetal(db, year_from, year_to)
