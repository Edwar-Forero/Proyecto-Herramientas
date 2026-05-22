"""Predicciones con regresión lineal (scikit-learn) sobre series agregadas."""

from __future__ import annotations

import numpy as np
from motor.motor_asyncio import AsyncIOMotorDatabase
from sklearn.linear_model import LinearRegression

from app.schemas.common import PredictionPoint, SeriesPoint
from app.schemas.dashboard import AnalyticsPredictionsResponse
from app.services.mortalidad_service import mortalidad_service
from app.services.natalidad_service import natalidad_service


class PredictionService:
    FORECAST_YEARS = 3

    def _forecast(self, series: list[SeriesPoint], metric: str) -> list[PredictionPoint]:
        if len(series) < 2:
            return []

        years = np.array([p.year for p in series], dtype=float).reshape(-1, 1)
        values = np.array([p.value for p in series], dtype=float)

        model = LinearRegression()
        model.fit(years, values)

        last_year = int(series[-1].year)
        predictions: list[PredictionPoint] = []
        for i in range(1, self.FORECAST_YEARS + 1):
            y = last_year + i
            pred = float(model.predict(np.array([[y]], dtype=float))[0])
            predictions.append(
                PredictionPoint(
                    year=y,
                    predicted=max(0, round(pred, 0)),
                    metric=metric,
                    confidence=round(model.score(years, values), 3),
                )
            )
        return predictions

    async def predict(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = 2020,
        year_to: int | None = 2024,
    ) -> AnalyticsPredictionsResponse:
        births = await natalidad_service.by_year(db, year_from, year_to)
        fetal, no_fetal = await mortalidad_service.by_year(db, year_from, year_to)

        trained_years = sorted({p.year for p in births})

        return AnalyticsPredictionsResponse(
            natalidad=self._forecast(births, "Nacimientos"),
            mortalidad_fetal=self._forecast(fetal, "Defunciones fetales"),
            mortalidad_no_fetal=self._forecast(no_fetal, "Defunciones no fetales"),
            model="LinearRegression (scikit-learn)",
            trained_on_years=trained_years,
        )


prediction_service = PredictionService()
