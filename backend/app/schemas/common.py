from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class MessageResponse(BaseModel):
    message: str


class YearFilter(BaseModel):
    year_from: int | None = Field(None, ge=2020, le=2030)
    year_to: int | None = Field(None, ge=2020, le=2030)
    department: str | None = None


class PaginatedResponse(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int
    page_size: int
    pages: int


class KPIItem(BaseModel):
    label: str
    value: int | float
    change_pct: float | None = None
    unit: str | None = None


class SeriesPoint(BaseModel):
    year: int
    value: int
    label: str | None = None


class CategoryPoint(BaseModel):
    category: str
    value: int
    percentage: float | None = None


class DepartmentPoint(BaseModel):
    department: str
    value: int
    rank: int | None = None


class ComparisonItem(BaseModel):
    metric: str
    natalidad: int
    mortalidad_fetal: int
    mortalidad_no_fetal: int


class PredictionPoint(BaseModel):
    year: int
    predicted: float
    metric: str
    confidence: float | None = None


class SystemStatus(BaseModel):
    database: str
    collections: dict[str, int]
    total_records: int
    status: str
