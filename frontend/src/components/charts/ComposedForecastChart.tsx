/**
 * ComposedForecastChart
 * Muestra en una sola gráfica:
 *   - Datos históricos reales (área sólida)
 *   - Proyección futura (línea punteada + puntos resaltados)
 *   - Línea vertical que separa "histórico" de "proyección"
 */
import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  Legend,
} from "recharts";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { formatNumber } from "@/lib/utils";

interface HistoricalPoint {
  year: number;
  value: number;
}

interface ForecastPoint {
  year: number;
  predicted: number;
  confidence?: number | null;
}

interface Props {
  title: string;
  description?: string;
  historical: HistoricalPoint[];
  forecast: ForecastPoint[];
  color?: string;
  r2?: number | null;
}

export function ComposedForecastChart({
  title,
  description,
  historical,
  forecast,
  color = "#818cf8",
  r2,
}: Props) {
  // Combinar datos históricos y proyecciones en una sola serie
  const chartData = [
    ...historical.map((p) => ({
      name: String(p.year),
      real: p.value,
      proyeccion: null as number | null,
    })),
    ...forecast.map((p) => ({
      name: String(p.year),
      real: null as number | null,
      proyeccion: p.predicted,
    })),
  ];

  const lastHistoricalYear = historical.length
    ? String(historical[historical.length - 1].year)
    : undefined;

  // Color un poco más tenue para la proyección
  const forecastColor = color + "aa";

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          {title}
          {r2 != null && (
            <span
              className="ml-auto text-xs font-normal px-2 py-0.5 rounded-full"
              style={{
                background: r2 > 0.85 ? "#166534" : r2 > 0.6 ? "#854d0e" : "#7f1d1d",
                color: "#f0fdf4",
              }}
            >
              R² = {r2.toFixed(3)}
            </span>
          )}
        </CardTitle>
        {description && <CardDescription>{description}</CardDescription>}
        <CardDescription className="text-xs">
          Histórico (área) + proyección 3 años (línea punteada)
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id={`hist-${title}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={color} stopOpacity={0.45} />
                  <stop offset="100%" stopColor={color} stopOpacity={0} />
                </linearGradient>
                <linearGradient id={`fore-${title}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={forecastColor} stopOpacity={0.3} />
                  <stop offset="100%" stopColor={forecastColor} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.3} />
              <XAxis dataKey="name" stroke="#94a3b8" fontSize={12} />
              <YAxis stroke="#94a3b8" fontSize={12} tickFormatter={(v) => formatNumber(v)} />
              <Tooltip
                contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
                formatter={(v: number, name: string) => [
                  formatNumber(v),
                  name === "real" ? "Histórico" : "Proyección",
                ]}
              />
              <Legend
                formatter={(value) => (value === "real" ? "Histórico" : "Proyección")}
              />
              {lastHistoricalYear && (
                <ReferenceLine
                  x={lastHistoricalYear}
                  stroke="#475569"
                  strokeDasharray="6 3"
                  label={{ value: "Hoy", fill: "#94a3b8", fontSize: 11 }}
                />
              )}
              <Area
                type="monotone"
                dataKey="real"
                name="real"
                stroke={color}
                fill={`url(#hist-${title})`}
                strokeWidth={2}
                connectNulls={false}
                dot={{ r: 4, fill: color }}
              />
              <Area
                type="monotone"
                dataKey="proyeccion"
                name="proyeccion"
                stroke={forecastColor}
                fill={`url(#fore-${title})`}
                strokeWidth={2}
                strokeDasharray="6 4"
                connectNulls={false}
                dot={{ r: 5, fill: forecastColor, strokeWidth: 2, stroke: color }}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
