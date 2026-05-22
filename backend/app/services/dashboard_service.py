from motor.motor_asyncio import AsyncIOMotorDatabase

from app.models import DEFUNCIONES_FETALES, DEFUNCIONES_NO_FETALES, NACIMIENTOS
from app.schemas.common import DepartmentPoint, KPIItem, SeriesPoint, SystemStatus
from app.schemas.dashboard import KPIsResponse, OverviewResponse
from app.services.aggregation_factory import AggregationPipelineFactory
from app.services.mortalidad_service import mortalidad_service
from app.services.natalidad_service import natalidad_service


class DashboardService:
    async def _collection_count(self, db: AsyncIOMotorDatabase, name: str) -> int:
        result = await db[name].aggregate([{"$count": "total"}]).to_list(length=1)
        return result[0]["total"] if result else 0

    async def kpis(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = 2020,
        year_to: int | None = 2024,
    ) -> KPIsResponse:
        births = await natalidad_service.by_year(db, year_from, year_to)
        fetal, no_fetal = await mortalidad_service.by_year(db, year_from, year_to)

        total_births = sum(p.value for p in births)
        total_fetal = sum(p.value for p in fetal)
        total_no_fetal = sum(p.value for p in no_fetal)
        total_events = total_births + total_fetal + total_no_fetal

        birth_trend = self._trend_pct(births)
        fetal_trend = self._trend_pct(fetal)

        kpis = [
            KPIItem(label="Nacimientos", value=total_births, change_pct=birth_trend, unit="registros"),
            KPIItem(label="Defunciones fetales", value=total_fetal, change_pct=fetal_trend, unit="registros"),
            KPIItem(label="Defunciones no fetales", value=total_no_fetal, unit="registros"),
            KPIItem(label="Total eventos vitales", value=total_events, unit="registros"),
            KPIItem(
                label="Tasa fetal / nacimientos (%)",
                value=round(total_fetal / total_births * 100, 2) if total_births else 0,
                unit="%",
            ),
        ]
        return KPIsResponse(
            kpis=kpis,
            period=f"{year_from or 2020}–{year_to or 2024}",
        )

    async def overview(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = 2020,
        year_to: int | None = 2024,
    ) -> OverviewResponse:
        kpi_resp = await self.kpis(db, year_from, year_to)
        births_by_year = await natalidad_service.by_year(db, year_from, year_to)
        fetal, no_fetal = await mortalidad_service.by_year(db, year_from, year_to)

        deaths_by_year: list[SeriesPoint] = []
        years = sorted({p.year for p in fetal + no_fetal + births_by_year})
        fetal_map = {p.year: p.value for p in fetal}
        no_map = {p.year: p.value for p in no_fetal}
        for y in years:
            deaths_by_year.append(
                SeriesPoint(
                    year=y,
                    value=fetal_map.get(y, 0) + no_map.get(y, 0),
                    label="Defunciones totales",
                )
            )

        top_departments = await natalidad_service.by_department(db, limit=8, year_from=year_from, year_to=year_to)

        return OverviewResponse(
            kpis=kpi_resp.kpis,
            births_by_year=births_by_year,
            deaths_by_year=deaths_by_year,
            top_departments=top_departments,
        )

    async def system_status(self, db: AsyncIOMotorDatabase) -> SystemStatus:
        collections = {
            NACIMIENTOS: await self._collection_count(db, NACIMIENTOS),
            DEFUNCIONES_FETALES: await self._collection_count(db, DEFUNCIONES_FETALES),
            DEFUNCIONES_NO_FETALES: await self._collection_count(db, DEFUNCIONES_NO_FETALES),
        }
        total = sum(collections.values())
        return SystemStatus(
            database=db.name,
            collections=collections,
            total_records=total,
            status="ok" if total > 0 else "empty",
        )

    @staticmethod
    def _trend_pct(series: list[SeriesPoint]) -> float | None:
        if len(series) < 2:
            return None
        first, last = series[0].value, series[-1].value
        if first == 0:
            return None
        return round((last - first) / first * 100, 2)


dashboard_service = DashboardService()
