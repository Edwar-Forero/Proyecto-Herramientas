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

    def _forecast(
        self, series: list[SeriesPoint], metric: str
    ) -> tuple[list[PredictionPoint], float | None]:
        """Entrena un modelo de regresión lineal y devuelve (predicciones, R²).

        Devuelve una tupla (predicciones, r2) donde r2 es el coeficiente de
        determinación del modelo entrenado. Un R² cercano a 1 indica que la
        tendencia histórica es bien modelada por una recta.
        """
        if len(series) < 2:
            return [], None

        years = np.array([p.year for p in series], dtype=float).reshape(-1, 1)
        values = np.array([p.value for p in series], dtype=float)

        model = LinearRegression()
        model.fit(years, values)
        r2 = round(float(model.score(years, values)), 3)

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
                    confidence=r2,
                )
            )
        return predictions, r2

    async def predict(
        self,
        db: AsyncIOMotorDatabase,
        year_from: int | None = 2020,
        year_to: int | None = 2024,
    ) -> AnalyticsPredictionsResponse:
        births = await natalidad_service.by_year(db, year_from, year_to)
        fetal, no_fetal = await mortalidad_service.by_year(db, year_from, year_to)

        trained_years = sorted({p.year for p in births})

        nat_preds, r2_nat = self._forecast(births, "Nacimientos")
        fetal_preds, r2_fetal = self._forecast(fetal, "Defunciones fetales")
        nofetal_preds, r2_nofetal = self._forecast(no_fetal, "Defunciones no fetales")

        return AnalyticsPredictionsResponse(
            natalidad=nat_preds,
            mortalidad_fetal=fetal_preds,
            mortalidad_no_fetal=nofetal_preds,
            historical_natalidad=births,
            historical_fetal=fetal,
            historical_no_fetal=no_fetal,
            model="Regresión Lineal (scikit-learn)",
            trained_on_years=trained_years,
            r2_natalidad=r2_nat,
            r2_fetal=r2_fetal,
            r2_no_fetal=r2_nofetal,
        )


prediction_service = PredictionService()
