"""Helpers de consultas comunes de MongoDB."""

from app.services.aggregation_factory import AggregationPipelineFactory

def build_summary_query(year_from: int | None = None, year_to: int | None = None):
    """Retorna una consulta rápida de agregación de conteo por año."""
    return AggregationPipelineFactory.count_by_year(year_from, year_to)
