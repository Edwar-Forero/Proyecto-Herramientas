from motor.motor_asyncio import AsyncIOMotorDatabase

from app.schemas.common import ComparisonItem
from app.schemas.dashboard import AnalyticsComparisonResponse
from app.services.mortalidad_service import mortalidad_service
from app.services.natalidad_service import natalidad_service


class AnalyticsService:
    async def comparison(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = 2020,
        year_to: int | None = 2024,
    ) -> AnalyticsComparisonResponse:
        births = await natalidad_service.by_year(db, year_from, year_to)
        fetal, no_fetal = await mortalidad_service.by_year(db, year_from, year_to)

        years = sorted({p.year for p in births + fetal + no_fetal})
        birth_map = {p.year: p.value for p in births}
        fetal_map = {p.year: p.value for p in fetal}
        no_map = {p.year: p.value for p in no_fetal}

        comparisons = [
            ComparisonItem(
                metric=str(y),
                natalidad=birth_map.get(y, 0),
                mortalidad_fetal=fetal_map.get(y, 0),
                mortalidad_no_fetal=no_map.get(y, 0),
            )
            for y in years
        ]
        return AnalyticsComparisonResponse(comparisons=comparisons)


analytics_service = AnalyticsService()
