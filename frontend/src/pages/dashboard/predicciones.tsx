import { useEffect, useState } from "react";
import { DashboardLayout } from "@/layouts/DashboardLayout";
import { ComposedForecastChart } from "@/components/charts/ComposedForecastChart";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { getPredictions } from "@/services/dashboard";
import { useFilterStore } from "@/store/filterStore";
import { getErrorMessage } from "@/services/api";
import { formatNumber } from "@/lib/utils";
import type { PredictionsResponse } from "@/types/api";

function R2Badge({ value }: { value: number | null | undefined }) {
  if (value == null) return <span className="text-muted-foreground text-xs">N/A</span>;
  const color =
    value > 0.85
      ? "text-emerald-400"
      : value > 0.6
      ? "text-amber-400"
      : "text-rose-400";
  const label =
    value > 0.85 ? "Buen ajuste" : value > 0.6 ? "Ajuste moderado" : "Ajuste bajo";
  return (
    <span className={`text-sm font-medium ${color}`}>
      {value.toFixed(3)} <span className="text-xs font-normal opacity-70">({label})</span>
    </span>
  );
}

export default function PrediccionesPage() {
  const params = useFilterStore((s) => s.params);
  const [data, setData] = useState<PredictionsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    getPredictions(params)
      .then(setData)
      .catch((e) => setError(getErrorMessage(e)))
      .finally(() => setLoading(false));
  }, [params.year_from, params.year_to]);

  return (
    <DashboardLayout
      title="Predicciones"
      subtitle="Proyecciones estadísticas con regresión lineal (scikit-learn)"
      allowedRoles={["admin", "analista"]}
    >
      {error && <p className="mb-4 text-sm text-rose-400">{error}</p>}

      {loading ? (
        <div className="grid gap-6 lg:grid-cols-1">
          <Skeleton className="h-80 w-full" />
          <Skeleton className="h-80 w-full" />
          <Skeleton className="h-80 w-full" />
        </div>
      ) : data ? (
        <>
          {/* Gráficas combinadas histórico + proyección */}
          <div className="grid gap-6 mb-6">
            <ComposedForecastChart
              title="Nacimientos — Histórico & Proyección"
              description="Tendencia real 2020–2024 y proyección a 3 años"
              historical={data.historical_natalidad ?? []}
              forecast={data.natalidad}
              color="#818cf8"
              r2={data.r2_natalidad}
            />
          </div>
          <div className="grid gap-6 lg:grid-cols-2 mb-6">
            <ComposedForecastChart
              title="Mortalidad Fetal — Histórico & Proyección"
              description="Defunciones fetales reales y proyectadas"
              historical={data.historical_fetal ?? []}
              forecast={data.mortalidad_fetal}
              color="#fb7185"
              r2={data.r2_fetal}
            />
            <ComposedForecastChart
              title="Mortalidad No Fetal — Histórico & Proyección"
              description="Defunciones no fetales reales y proyectadas"
              historical={data.historical_no_fetal ?? []}
              forecast={data.mortalidad_no_fetal}
              color="#fbbf24"
              r2={data.r2_no_fetal}
            />
          </div>

          {/* Tabla de valores proyectados con confianza */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">
                Tabla de valores proyectados —{" "}
                <span className="text-muted-foreground font-normal">Modelo: {data.model}</span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              {/* R² summary */}
              <div className="grid grid-cols-3 gap-4 mb-6 text-center">
                <div className="rounded-lg border border-border p-4">
                  <p className="text-xs text-muted-foreground mb-1">R² Nacimientos</p>
                  <R2Badge value={data.r2_natalidad} />
                </div>
                <div className="rounded-lg border border-border p-4">
                  <p className="text-xs text-muted-foreground mb-1">R² Mortalidad Fetal</p>
                  <R2Badge value={data.r2_fetal} />
                </div>
                <div className="rounded-lg border border-border p-4">
                  <p className="text-xs text-muted-foreground mb-1">R² Mortalidad No Fetal</p>
                  <R2Badge value={data.r2_no_fetal} />
                </div>
              </div>

              <div className="overflow-auto rounded-lg border border-border">
                <table className="w-full text-sm">
                  <thead className="bg-muted/40">
                    <tr>
                      <th className="px-4 py-3 text-left font-medium text-muted-foreground">Año</th>
                      <th className="px-4 py-3 text-right font-medium text-muted-foreground">Nacimientos</th>
                      <th className="px-4 py-3 text-right font-medium text-muted-foreground">Def. fetales</th>
                      <th className="px-4 py-3 text-right font-medium text-muted-foreground">Def. no fetales</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {data.natalidad.map((n, i) => (
                      <tr key={n.year} className="hover:bg-muted/20 transition-colors">
                        <td className="px-4 py-3 font-semibold">{n.year}</td>
                        <td className="px-4 py-3 text-right text-indigo-400">
                          {formatNumber(n.predicted)}
                        </td>
                        <td className="px-4 py-3 text-right text-rose-400">
                          {formatNumber(data.mortalidad_fetal[i]?.predicted ?? 0)}
                        </td>
                        <td className="px-4 py-3 text-right text-amber-400">
                          {formatNumber(data.mortalidad_no_fetal[i]?.predicted ?? 0)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <p className="text-xs text-muted-foreground mt-3">
                Entrenado con datos de los años: {data.trained_on_years.join(", ")}. El coeficiente R² mide
                qué tan bien se ajusta la recta de regresión a los datos históricos (0 = ajuste nulo, 1 = ajuste perfecto).
              </p>
            </CardContent>
          </Card>
        </>
      ) : null}
    </DashboardLayout>
  );
}
