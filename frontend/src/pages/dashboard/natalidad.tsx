import { useEffect, useState } from "react";
import { DashboardLayout } from "@/layouts/DashboardLayout";
import { LineChartCard } from "@/components/charts/LineChartCard";
import { PieChartCard } from "@/components/charts/PieChartCard";
import { BarChartCard } from "@/components/charts/BarChartCard";
import { Skeleton } from "@/components/ui/skeleton";
import {
  getNatalidadByYear,
  getNatalidadByDepartment,
  getNatalidadByGender,
  getNatalidadEducation,
} from "@/services/dashboard";
import { useFilterStore } from "@/store/filterStore";
import { getErrorMessage } from "@/services/api";
import type { CategoryPoint, DepartmentPoint, SeriesPoint } from "@/types/api";

export default function NatalidadPage() {
  const params = useFilterStore((s) => s.params);
  const [byYear, setByYear] = useState<SeriesPoint[]>([]);
  const [byDept, setByDept] = useState<DepartmentPoint[]>([]);
  const [byGender, setByGender] = useState<CategoryPoint[]>([]);
  const [byEducation, setByEducation] = useState<CategoryPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    Promise.all([
      getNatalidadByYear(params),
      getNatalidadByDepartment(params),
      getNatalidadByGender(params),
      getNatalidadEducation(params),
    ])
      .then(([y, d, g, e]) => {
        setByYear(y);
        setByDept(d);
        setByGender(g);
        setByEducation(e);
      })
      .catch((err) => setError(getErrorMessage(err)))
      .finally(() => setLoading(false));
  }, [params.year_from, params.year_to]);

  return (
    <DashboardLayout title="Natalidad" subtitle="Análisis de nacimientos registrados">
      {error && <p className="mb-4 text-sm text-rose-400">{error}</p>}
      {loading ? (
        <Skeleton className="h-96 w-full" />
      ) : (
        <div className="grid gap-6 lg:grid-cols-2">
          <LineChartCard title="Nacimientos por año" data={byYear} />
          <PieChartCard
            title="Distribución por sexo"
            data={byGender.map((g) => ({ name: g.category, value: g.value }))}
          />
          <BarChartCard
            title="Top departamentos"
            data={byDept.map((d) => ({ label: d.department, value: d.value }))}
          />
          <BarChartCard
            title="Nivel educativo materno"
            data={byEducation.map((e) => ({ label: e.category, value: e.value }))}
            color="#34d399"
          />
        </div>
      )}
    </DashboardLayout>
  );
}
