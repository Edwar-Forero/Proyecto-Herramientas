"""Factory para pipelines de agregación MongoDB."""

from __future__ import annotations

from typing import Any


class AggregationPipelineFactory:
    """Construye pipelines reutilizables — patrón Factory."""

    @staticmethod
    def match_filters(
        year_from: int | None = None,
        year_to: int | None = None,
        department: str | None = None,
        dept_field: str = "cod_dpto_desc",
    ) -> dict[str, Any]:
        match: dict[str, Any] = {}
        if year_from is not None or year_to is not None:
            ano: dict[str, Any] = {}
            if year_from is not None:
                ano["$gte"] = year_from
            if year_to is not None:
                ano["$lte"] = year_to
            match["ano"] = ano
        if department:
            match[dept_field] = department
        return match

    @classmethod
    def count_by_year(
        cls,
        year_from: int | None = None,
        year_to: int | None = None,
        department: str | None = None,
    ) -> list[dict[str, Any]]:
        match = cls.match_filters(year_from, year_to, department)
        pipeline: list[dict[str, Any]] = []
        if match:
            pipeline.append({"$match": match})
        pipeline.extend(
            [
                {"$group": {"_id": "$ano", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 0, "year": "$_id", "value": "$count"}},
            ]
        )
        return pipeline

    @classmethod
    def count_by_field(
        cls,
        field: str,
        limit: int = 15,
        year_from: int | None = None,
        year_to: int | None = None,
        department: str | None = None,
    ) -> list[dict[str, Any]]:
        match = cls.match_filters(year_from, year_to, department)
        pipeline: list[dict[str, Any]] = []
        if match:
            pipeline.append({"$match": match})
        pipeline.extend(
            [
                {"$match": {field: {"$nin": [None, "", "NA", "N/A"]}}},
                {"$group": {"_id": f"${field}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": limit},
                {"$project": {"_id": 0, "category": "$_id", "value": "$count"}},
            ]
        )
        return pipeline

    @classmethod
    def count_by_department(
        cls,
        limit: int = 10,
        year_from: int | None = None,
        year_to: int | None = None,
        dept_field: str = "cod_dpto_desc",
    ) -> list[dict[str, Any]]:
        match = cls.match_filters(year_from, year_to)
        pipeline: list[dict[str, Any]] = []
        if match:
            pipeline.append({"$match": match})
        pipeline.extend(
            [
                {"$match": {dept_field: {"$nin": [None, "", "NA"]}}},
                {"$group": {"_id": f"${dept_field}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": limit},
                {"$project": {"_id": 0, "department": "$_id", "value": "$count"}},
            ]
        )
        return pipeline

    @staticmethod
    def total_count(match: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = []
        if match:
            pipeline.append({"$match": match})
        pipeline.append({"$count": "total"})
        return pipeline
