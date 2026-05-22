from motor.motor_asyncio import AsyncIOMotorDatabase

from app.models import NACIMIENTOS
from app.schemas.common import CategoryPoint, DepartmentPoint, SeriesPoint
from app.services.aggregation_factory import AggregationPipelineFactory


class NatalidadService:
    async def by_year(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = None,
        year_to: int | None = None,
        department: str | None = None,
    ) -> list[SeriesPoint]:
        pipeline = AggregationPipelineFactory.count_by_year(year_from, year_to, department)
        rows = await db[NACIMIENTOS].aggregate(pipeline).to_list(length=20)
        return [SeriesPoint(year=r["year"], value=r["value"], label="Nacimientos") for r in rows]

    async def by_department(
        self,
        db: AsyncIOMotorDatabase,
        limit: int = 10,
        year_from: int | None = None,
        year_to: int | None = None,
    ) -> list[DepartmentPoint]:
        pipeline = AggregationPipelineFactory.count_by_department(limit, year_from, year_to)
        rows = await db[NACIMIENTOS].aggregate(pipeline).to_list(length=limit)
        return [
            DepartmentPoint(department=r["department"], value=r["value"], rank=i + 1)
            for i, r in enumerate(rows)
        ]

    async def by_gender(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = None,
        year_to: int | None = None,
    ) -> list[CategoryPoint]:
        pipeline = AggregationPipelineFactory.count_by_field(
            "sexo_desc", limit=10, year_from=year_from, year_to=year_to
        )
        rows = await db[NACIMIENTOS].aggregate(pipeline).to_list(length=10)
        total = sum(r["value"] for r in rows) or 1
        return [
            CategoryPoint(
                category=r["category"],
                value=r["value"],
                percentage=round(r["value"] / total * 100, 2),
            )
            for r in rows
        ]

    async def by_education(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = None,
        year_to: int | None = None,
    ) -> list[CategoryPoint]:
        pipeline = AggregationPipelineFactory.count_by_field(
            "niv_edum_desc", limit=12, year_from=year_from, year_to=year_to
        )
        rows = await db[NACIMIENTOS].aggregate(pipeline).to_list(length=12)
        return [CategoryPoint(category=r["category"], value=r["value"]) for r in rows]

    async def by_health_regime(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = None,
        year_to: int | None = None,
    ) -> list[CategoryPoint]:
        pipeline = AggregationPipelineFactory.count_by_field(
            "seg_social_desc", limit=10, year_from=year_from, year_to=year_to
        )
        rows = await db[NACIMIENTOS].aggregate(pipeline).to_list(length=10)
        return [CategoryPoint(category=r["category"], value=r["value"]) for r in rows]

    async def by_maternal_age(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = None,
        year_to: int | None = None,
    ) -> list[CategoryPoint]:
        pipeline = AggregationPipelineFactory.count_by_field(
            "edad_madre_desc", limit=15, year_from=year_from, year_to=year_to
        )
        rows = await db[NACIMIENTOS].aggregate(pipeline).to_list(length=15)
        return [CategoryPoint(category=r["category"], value=r["value"]) for r in rows]


natalidad_service = NatalidadService()
