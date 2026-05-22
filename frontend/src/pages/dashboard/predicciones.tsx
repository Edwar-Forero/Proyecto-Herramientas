import { useEffect, useState } from "react";
import { DashboardLayout } from "@/layouts/DashboardLayout";
import { LineChartCard } from "@/components/charts/LineChartCard";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { getPredictions } from "@/services/dashboard";
import { useFilterStore } from "@/store/filterStore";
import { getErrorMessage } from "@/services/api";

export default function PrediccionesPage() {
  const params = useFilterStore((s) => s.params);
  const [data, setData] = useState<Awaited<ReturnType<typeof getPredictions>> | null>(null);
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
      subtitle="Proyecciones con regresión lineal (ML)"
      allowedRoles={["admin", "analista"]}
    >
      {error && <p className="mb-4 text-sm text-rose-400">{error}</p>}
      {loading ? (
        <Skeleton className="h-96 w-full" />
      ) : data ? (
        <>
          <Card className="mb-6">
            <CardHeader>
              <CardTitle className="text-base">Modelo: {data.model}</CardTitle>
            </CardHeader>
            <CardContent className="text-sm text-muted-foreground">
              Entrenado con años: {data.trained_on_years.join(", ")}. Proyección a 3 años futuros.
            </CardContent>
          </Card>
          <div className="grid gap-6 lg:grid-cols-3">
            <LineChartCard
              title="Proyección nacimientos"
              data={data.natalidad.map((p) => ({ year: p.year, value: p.predicted }))}
              color="#818cf8"
            />
            <LineChartCard
              title="Proyección mortalidad fetal"
              data={data.mortalidad_fetal.map((p) => ({ year: p.year, value: p.predicted }))}
              color="#fb7185"
            />
            <LineChartCard
              title="Proyección mortalidad no fetal"
              data={data.mortalidad_no_fetal.map((p) => ({ year: p.year, value: p.predicted }))}
              color="#fbbf24"
            />
          </div>
        </>
      ) : null}
    </DashboardLayout>
  );
}
