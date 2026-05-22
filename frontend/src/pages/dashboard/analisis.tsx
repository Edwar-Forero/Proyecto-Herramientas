import { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { DashboardLayout } from "@/layouts/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { getComparison } from "@/services/dashboard";
import { useFilterStore } from "@/store/filterStore";
import { getErrorMessage, api } from "@/services/api";
import { formatNumber } from "@/lib/utils";
import type { ComparisonItem } from "@/types/api";
import { Download } from "lucide-react";

export default function AnalisisPage() {
  const params = useFilterStore((s) => s.params);
  const [data, setData] = useState<ComparisonItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    getComparison(params)
      .then(setData)
      .catch((e) => setError(getErrorMessage(e)))
      .finally(() => setLoading(false));
  }, [params.year_from, params.year_to]);

  const chartData = data.map((d) => ({
    year: d.metric,
    Natalidad: d.natalidad,
    "Mortalidad fetal": d.mortalidad_fetal,
    "Mortalidad no fetal": d.mortalidad_no_fetal,
  }));

  async function handleExport() {
    const q = new URLSearchParams();
    if (params.year_from) q.set("year_from", String(params.year_from));
    if (params.year_to) q.set("year_to", String(params.year_to));
    const res = await api.get(`/analytics/export/comparison?${q}`, { responseType: "blob" });
    const url = window.URL.createObjectURL(new Blob([res.data]));
    const a = document.createElement("a");
    a.href = url;
    a.download = "comparacion_vitales.csv";
    a.click();
  }

  return (
    <DashboardLayout
      title="Análisis comparativo"
      subtitle="Comparación entre tipos de eventos vitales"
      allowedRoles={["admin", "analista"]}
    >
      <div className="mb-4 flex justify-end">
        <Button variant="outline" size="sm" onClick={handleExport} type="button">
          <Download className="h-4 w-4 mr-2" />
          Exportar CSV
        </Button>
      </div>
      {error && <p className="mb-4 text-sm text-rose-400">{error}</p>}
      {loading ? (
        <Skeleton className="h-96 w-full" />
      ) : (
        <Card>
          <CardHeader>
            <CardTitle>Comparación anual</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-96">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.3} />
                  <XAxis dataKey="year" stroke="#94a3b8" />
                  <YAxis stroke="#94a3b8" tickFormatter={(v) => formatNumber(v)} />
                  <Tooltip
                    contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
                    formatter={(v: number) => formatNumber(v)}
                  />
                  <Legend />
                  <Bar dataKey="Natalidad" fill="#818cf8" radius={[4, 4, 0, 0]} />
                  <Bar dataKey="Mortalidad fetal" fill="#fb7185" radius={[4, 4, 0, 0]} />
                  <Bar dataKey="Mortalidad no fetal" fill="#fbbf24" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      )}
    </DashboardLayout>
  );
}
