from app.services.aggregation_factory import AggregationPipelineFactory

def build_summary_query(year_from: int | None = None, year_to: int | None = None):
    return AggregationPipelineFactory.count_by_year(year_from, year_to)
