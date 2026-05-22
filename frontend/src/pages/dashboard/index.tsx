import { useEffect, useState } from "react";
import { DashboardLayout } from "@/layouts/DashboardLayout";
import { KpiCard } from "@/components/dashboard/KpiCard";
import { LineChartCard } from "@/components/charts/LineChartCard";
import { BarChartCard } from "@/components/charts/BarChartCard";
import { Skeleton } from "@/components/ui/skeleton";
import { getOverview } from "@/services/dashboard";
import { useFilterStore } from "@/store/filterStore";
import type { OverviewResponse } from "@/types/api";
import { getErrorMessage } from "@/services/api";

export default function DashboardOverview() {
  const params = useFilterStore((s) => s.params);
  const [data, setData] = useState<OverviewResponse | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    getOverview(params)
      .then(setData)
      .catch((e) => setError(getErrorMessage(e)))
      .finally(() => setLoading(false));
  }, [params.year_from, params.year_to, params.department]);

  return (
    <DashboardLayout title="Overview" subtitle="Resumen ejecutivo de eventos vitales">
      {error && <p className="mb-4 text-sm text-rose-400">{error}</p>}
      {loading ? (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          {[...Array(4)].map((_, i) => (
            <Skeleton key={i} className="h-28" />
          ))}
        </div>
      ) : data ? (
        <>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4 mb-6">
            {data.kpis.map((kpi, i) => (
              <KpiCard key={kpi.label} kpi={kpi} index={i} />
            ))}
          </div>
          <div className="grid gap-6 lg:grid-cols-2">
            <LineChartCard
              title="Nacimientos por año"
              data={data.births_by_year}
              color="#818cf8"
            />
            <LineChartCard
              title="Defunciones totales por año"
              data={data.deaths_by_year}
              color="#f472b6"
            />
            <BarChartCard
              title="Top departamentos — nacimientos"
              data={data.top_departments.map((d) => ({
                label: d.department,
                value: d.value,
              }))}
            />
          </div>
        </>
      ) : null}
    </DashboardLayout>
  );
}
