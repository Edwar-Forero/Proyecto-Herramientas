from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import StreamingResponse
from motor.motor_asyncio import AsyncIOMotorDatabase
import csv
import io

from app.database import get_db
from app.schemas.dashboard import AnalyticsComparisonResponse, AnalyticsPredictionsResponse
from app.services.analytics_service import analytics_service
from app.services.prediction_service import prediction_service
from app.utils.dependencies import AnalistaUser

router = APIRouter(prefix="/analytics", tags=["Analítica"])


@router.get("/comparison", response_model=AnalyticsComparisonResponse)
async def comparison(
    current_user: AnalistaUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(2020, ge=2020, le=2030),
    year_to: int | None = Query(2024, ge=2020, le=2030),
):
    return await analytics_service.comparison(db, year_from, year_to)


@router.get("/predictions", response_model=AnalyticsPredictionsResponse)
async def predictions(
    current_user: AnalistaUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(2020, ge=2020, le=2030),
    year_to: int | None = Query(2024, ge=2020, le=2030),
):
    return await prediction_service.predict(db, year_from, year_to)


@router.get("/export/comparison")
async def export_comparison(
    current_user: AnalistaUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    year_from: int | None = Query(2020, ge=2020, le=2030),
    year_to: int | None = Query(2024, ge=2020, le=2030),
):
    result = await analytics_service.comparison(db, year_from, year_to)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["año", "natalidad", "mortalidad_fetal", "mortalidad_no_fetal"])
    for row in result.comparisons:
        writer.writerow([row.metric, row.natalidad, row.mortalidad_fetal, row.mortalidad_no_fetal])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=comparacion_vitales.csv"},
    )
