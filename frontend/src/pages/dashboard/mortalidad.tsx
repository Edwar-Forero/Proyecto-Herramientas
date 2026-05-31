import { useEffect, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { DashboardLayout } from "@/layouts/DashboardLayout";
import { LineChartCard } from "@/components/charts/LineChartCard";
import { BarChartCard } from "@/components/charts/BarChartCard";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { getMortalidadByYear } from "@/services/dashboard";
import { api } from "@/services/api";
import { useFilterStore } from "@/store/filterStore";
import { getErrorMessage } from "@/services/api";
import { formatNumber } from "@/lib/utils";
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

  // Datos combinados para la gráfica de composición apilada
  const stackedData = (() => {
    const fetalMap = Object.fromEntries(fetal.map((p) => [p.year, p.value]));
    const noFetalMap = Object.fromEntries(noFetal.map((p) => [p.year, p.value]));
    const years = [...new Set([...fetal.map((p) => p.year), ...noFetal.map((p) => p.year)])].sort();
    return years.map((y) => ({
      year: String(y),
      "Fetal": fetalMap[y] ?? 0,
      "No fetal": noFetalMap[y] ?? 0,
    }));
  })();

  return (
    <DashboardLayout title="Mortalidad" subtitle="Defunciones fetales y no fetales en Colombia">
      {error && <p className="mb-4 text-sm text-rose-400">{error}</p>}
      {loading ? (
        <div className="grid gap-6 lg:grid-cols-2">
          {[...Array(5)].map((_, i) => <Skeleton key={i} className="h-80 w-full" />)}
        </div>
      ) : (
        <div className="grid gap-6">
          {/* Gráfica de composición apilada (NUEVA) */}
          <Card>
            <CardHeader>
              <CardTitle>Composición de defunciones por año</CardTitle>
              <CardDescription>
                Comparación acumulada entre defunciones fetales y no fetales
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="h-80">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={stackedData}>
                    <defs>
                      <linearGradient id="gradFetal" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#fb7185" stopOpacity={0.5} />
                        <stop offset="100%" stopColor="#fb7185" stopOpacity={0.05} />
                      </linearGradient>
                      <linearGradient id="gradNoFetal" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#fbbf24" stopOpacity={0.5} />
                        <stop offset="100%" stopColor="#fbbf24" stopOpacity={0.05} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.3} />
                    <XAxis dataKey="year" stroke="#94a3b8" fontSize={12} />
                    <YAxis stroke="#94a3b8" fontSize={12} tickFormatter={(v) => formatNumber(v)} />
                    <Tooltip
                      contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
                      formatter={(v: number) => formatNumber(v)}
                    />
                    <Legend />
                    <Area
                      type="monotone"
                      dataKey="Fetal"
                      stackId="1"
                      stroke="#fb7185"
                      fill="url(#gradFetal)"
                      strokeWidth={2}
                    />
                    <Area
                      type="monotone"
                      dataKey="No fetal"
                      stackId="1"
                      stroke="#fbbf24"
                      fill="url(#gradNoFetal)"
                      strokeWidth={2}
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>

          {/* Líneas individuales */}
          <div className="grid gap-6 lg:grid-cols-2">
            <LineChartCard title="Defunciones fetales por año" data={fetal} color="#fb7185" />
            <LineChartCard title="Defunciones no fetales por año" data={noFetal} color="#fbbf24" />
          </div>

          {/* Causas y grupos etarios */}
          <div className="grid gap-6 lg:grid-cols-2">
            <BarChartCard
              title="Situación de la defunción fetal"
              description="Clasificación según circunstancia de la defunción"
              data={causes.map((c) => ({ label: c.category, value: c.value }))}
              color="#fb7185"
            />
            <BarChartCard
              title="Grupos etarios — defunciones no fetales"
              description="Distribución de defunciones por rango de edad"
              data={ageGroups.map((a) => ({ label: a.category, value: a.value }))}
              color="#fbbf24"
            />
          </div>
        </div>
      )}
    </DashboardLayout>
  );
}
