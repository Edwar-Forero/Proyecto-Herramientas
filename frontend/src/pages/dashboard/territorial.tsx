import { useEffect, useState } from "react";
import { DashboardLayout } from "@/layouts/DashboardLayout";
import { BarChartCard } from "@/components/charts/BarChartCard";
import { Skeleton } from "@/components/ui/skeleton";
import { getMortalidadByRegion, getNatalidadByDepartment } from "@/services/dashboard";
import { useFilterStore } from "@/store/filterStore";
import { getErrorMessage } from "@/services/api";
import type { DepartmentPoint } from "@/types/api";

export default function TerritorialPage() {
  const params = useFilterStore((s) => s.params);
  const [births, setBirths] = useState<DepartmentPoint[]>([]);
  const [fetal, setFetal] = useState<DepartmentPoint[]>([]);
  const [noFetal, setNoFetal] = useState<DepartmentPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    Promise.all([getNatalidadByDepartment(params), getMortalidadByRegion(params)])
      .then(([b, m]) => {
        setBirths(b);
        setFetal(m.fetal);
        setNoFetal(m.no_fetal);
      })
      .catch((e) => setError(getErrorMessage(e)))
      .finally(() => setLoading(false));
  }, [params.year_from, params.year_to]);

  return (
    <DashboardLayout title="Análisis territorial" subtitle="Rankings y comparación regional">
      {error && <p className="mb-4 text-sm text-rose-400">{error}</p>}
      {loading ? (
        <Skeleton className="h-96 w-full" />
      ) : (
        <div className="grid gap-6">
          <BarChartCard
            title="Nacimientos por departamento"
            data={births.map((d) => ({ label: d.department, value: d.value }))}
          />
          <div className="grid gap-6 lg:grid-cols-2">
            <BarChartCard
              title="Defunciones fetales por departamento"
              data={fetal.map((d) => ({ label: d.department, value: d.value }))}
              color="#fb7185"
            />
            <BarChartCard
              title="Defunciones no fetales por departamento"
              data={noFetal.map((d) => ({ label: d.department, value: d.value }))}
              color="#fbbf24"
            />
          </div>
        </div>
      )}
    </DashboardLayout>
  );
}
