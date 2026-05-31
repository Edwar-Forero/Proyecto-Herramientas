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
  getNatalidadHealthRegime,
  getNatalidadMaternalAge,
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
  const [byRegime, setByRegime] = useState<CategoryPoint[]>([]);
  const [byMaternalAge, setByMaternalAge] = useState<CategoryPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    Promise.all([
      getNatalidadByYear(params),
      getNatalidadByDepartment(params),
      getNatalidadByGender(params),
      getNatalidadEducation(params),
      getNatalidadHealthRegime(params),
      getNatalidadMaternalAge(params),
    ])
      .then(([y, d, g, e, r, m]) => {
        setByYear(y);
        setByDept(d);
        setByGender(g);
        setByEducation(e);
        setByRegime(r);
        setByMaternalAge(m);
      })
      .catch((err) => setError(getErrorMessage(err)))
      .finally(() => setLoading(false));
  }, [params.year_from, params.year_to]);

  return (
    <DashboardLayout title="Natalidad" subtitle="Análisis completo de nacimientos registrados en Colombia">
      {error && <p className="mb-4 text-sm text-rose-400">{error}</p>}
      {loading ? (
        <div className="grid gap-6 lg:grid-cols-2">
          {[...Array(6)].map((_, i) => <Skeleton key={i} className="h-80 w-full" />)}
        </div>
      ) : (
        <div className="grid gap-6 lg:grid-cols-2">
          {/* Fila 1: tendencia temporal + distribución por sexo */}
          <LineChartCard
            title="Nacimientos por año"
            description="Tendencia anual de nacimientos registrados"
            data={byYear}
          />
          <PieChartCard
            title="Distribución por sexo del recién nacido"
            data={byGender.map((g) => ({ name: g.category, value: g.value }))}
          />

          {/* Fila 2: top departamentos + nivel educativo materno */}
          <BarChartCard
            title="Top departamentos — nacimientos"
            description="Departamentos con mayor registro de nacimientos"
            data={byDept.map((d) => ({ label: d.department, value: d.value }))}
          />
          <BarChartCard
            title="Nivel educativo de la madre"
            description="Distribución por escolaridad materna"
            data={byEducation.map((e) => ({ label: e.category, value: e.value }))}
            color="#34d399"
          />

          {/* Fila 3 (NUEVAS): régimen de salud + edad materna */}
          <PieChartCard
            title="Régimen de seguridad social materno"
            description="Tipo de afiliación al sistema de salud"
            data={byRegime.map((r) => ({ name: r.category, value: r.value }))}
          />
          <BarChartCard
            title="Nacimientos por edad de la madre"
            description="Grupos de edad materna al momento del parto"
            data={byMaternalAge.map((m) => ({ label: m.category, value: m.value }))}
            color="#f472b6"
          />
        </div>
      )}
    </DashboardLayout>
  );
}
