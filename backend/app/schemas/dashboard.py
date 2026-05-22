from pydantic import BaseModel

from app.schemas.common import CategoryPoint, ComparisonItem, DepartmentPoint, KPIItem, PredictionPoint, SeriesPoint


class KPIsResponse(BaseModel):
    kpis: list[KPIItem]
    period: str


class OverviewResponse(BaseModel):
    kpis: list[KPIItem]
    births_by_year: list[SeriesPoint]
    deaths_by_year: list[SeriesPoint]
    top_departments: list[DepartmentPoint]


class NatalidadByYearResponse(BaseModel):
    data: list[SeriesPoint]


class NatalidadByDepartmentResponse(BaseModel):
    data: list[DepartmentPoint]


class NatalidadByGenderResponse(BaseModel):
    data: list[CategoryPoint]


class MortalidadByYearResponse(BaseModel):
    fetal: list[SeriesPoint]
    no_fetal: list[SeriesPoint]


class MortalidadByRegionResponse(BaseModel):
    fetal: list[DepartmentPoint]
    no_fetal: list[DepartmentPoint]


class AnalyticsComparisonResponse(BaseModel):
    comparisons: list[ComparisonItem]


class AnalyticsPredictionsResponse(BaseModel):
    natalidad: list[PredictionPoint]
    mortalidad_fetal: list[PredictionPoint]
    mortalidad_no_fetal: list[PredictionPoint]
    model: str
    trained_on_years: list[int]
