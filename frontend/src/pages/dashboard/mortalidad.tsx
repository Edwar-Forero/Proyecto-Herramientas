import { useEffect, useState } from "react";
import { DashboardLayout } from "@/layouts/DashboardLayout";
import { LineChartCard } from "@/components/charts/LineChartCard";
import { BarChartCard } from "@/components/charts/BarChartCard";
import { Skeleton } from "@/components/ui/skeleton";
import { getMortalidadByYear } from "@/services/dashboard";
import { api } from "@/services/api";
import { useFilterStore } from "@/store/filterStore";
import { getErrorMessage } from "@/services/api";
import type { CategoryPoint, SeriesPoint } from "@/types/api";

export default function MortalidadPage() {
  const params = useFilterStore((s) => s.params);
  const [fetal, setFetal] = useState<SeriesPoint[]>([]);
  const [noFetal, setNoFetal] = useState<SeriesPoint[]>([]);
  const [causes, setCauses] = useState<CategoryPoint[]>([]);
  const [ageGroups, setAgeGroups] = useState<CategoryPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    const q = new URLSearchParams();
    if (params.year_from) q.set("year_from", String(params.year_from));
    if (params.year_to) q.set("year_to", String(params.year_to));
    const qs = q.toString() ? `?${q}` : "";

    Promise.all([
      getMortalidadByYear(params),
      api.get<CategoryPoint[]>(`/mortalidad/causes-fetal${qs}`),
      api.get<CategoryPoint[]>(`/mortalidad/age-groups${qs}`),
    ])
      .then(([years, causesRes, ageRes]) => {
        setFetal(years.fetal);
        setNoFetal(years.no_fetal);
        setCauses(causesRes.data);
        setAgeGroups(ageRes.data);
      })
      .catch((err) => setError(getErrorMessage(err)))
      .finally(() => setLoading(false));
  }, [params.year_from, params.year_to]);

  return (
    <DashboardLayout title="Mortalidad" subtitle="Defunciones fetales y no fetales">
      {error && <p className="mb-4 text-sm text-rose-400">{error}</p>}
      {loading ? (
        <Skeleton className="h-96 w-full" />
      ) : (
        <div className="grid gap-6 lg:grid-cols-2">
          <LineChartCard title="Defunciones fetales por año" data={fetal} color="#fb7185" />
          <LineChartCard title="Defunciones no fetales por año" data={noFetal} color="#fbbf24" />
          <BarChartCard
            title="Situación defunción fetal"
            data={causes.map((c) => ({ label: c.category, value: c.value }))}
            color="#fb7185"
          />
          <BarChartCard
            title="Grupos etarios (no fetal)"
            data={ageGroups.map((a) => ({ label: a.category, value: a.value }))}
            color="#fbbf24"
          />
        </div>
      )}
    </DashboardLayout>
  );
}
