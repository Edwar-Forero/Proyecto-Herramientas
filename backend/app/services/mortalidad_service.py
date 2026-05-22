from motor.motor_asyncio import AsyncIOMotorDatabase

from app.models import DEFUNCIONES_FETALES, DEFUNCIONES_NO_FETALES
from app.schemas.common import CategoryPoint, DepartmentPoint, SeriesPoint
from app.services.aggregation_factory import AggregationPipelineFactory


class MortalidadService:
    async def by_year(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = None,
        year_to: int | None = None,
    ) -> tuple[list[SeriesPoint], list[SeriesPoint]]:
        fetal_pipe = AggregationPipelineFactory.count_by_year(year_from, year_to)
        nofetal_pipe = AggregationPipelineFactory.count_by_year(year_from, year_to)

        fetal_rows = await db[DEFUNCIONES_FETALES].aggregate(fetal_pipe).to_list(length=20)
        nofetal_rows = await db[DEFUNCIONES_NO_FETALES].aggregate(nofetal_pipe).to_list(length=20)

        fetal = [SeriesPoint(year=r["year"], value=r["value"], label="Fetal") for r in fetal_rows]
        no_fetal = [
            SeriesPoint(year=r["year"], value=r["value"], label="No fetal") for r in nofetal_rows
        ]
        return fetal, no_fetal

    async def by_region(
        self,
        db: AsyncIOMotorDatabase,
        limit: int = 10,
        year_from: int | None = None,
        year_to: int | None = None,
    ) -> tuple[list[DepartmentPoint], list[DepartmentPoint]]:
        pipe = AggregationPipelineFactory.count_by_department(limit, year_from, year_to)
        fetal_rows = await db[DEFUNCIONES_FETALES].aggregate(pipe).to_list(length=limit)
        nofetal_rows = await db[DEFUNCIONES_NO_FETALES].aggregate(pipe).to_list(length=limit)

        fetal = [
            DepartmentPoint(department=r["department"], value=r["value"], rank=i + 1)
            for i, r in enumerate(fetal_rows)
        ]
        no_fetal = [
            DepartmentPoint(department=r["department"], value=r["value"], rank=i + 1)
            for i, r in enumerate(nofetal_rows)
        ]
        return fetal, no_fetal

    async def causes_fetal(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = None,
        year_to: int | None = None,
    ) -> list[CategoryPoint]:
        pipeline = AggregationPipelineFactory.count_by_field(
            "sit_defun_desc", limit=10, year_from=year_from, year_to=year_to
        )
        rows = await db[DEFUNCIONES_FETALES].aggregate(pipeline).to_list(length=10)
        return [CategoryPoint(category=r["category"], value=r["value"]) for r in rows]

    async def age_groups_nofetal(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = None,
        year_to: int | None = None,
    ) -> list[CategoryPoint]:
        pipeline = AggregationPipelineFactory.count_by_field(
            "gru_ed1_desc", limit=15, year_from=year_from, year_to=year_to
        )
        rows = await db[DEFUNCIONES_NO_FETALES].aggregate(pipeline).to_list(length=15)
        return [CategoryPoint(category=r["category"], value=r["value"]) for r in rows]


mortalidad_service = MortalidadService()
