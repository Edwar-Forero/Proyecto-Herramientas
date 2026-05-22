import { motion } from "framer-motion";
import { TrendingDown, TrendingUp } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { formatNumber } from "@/lib/utils";
import type { KPIItem } from "@/types/api";

export function KpiCard({ kpi, index }: { kpi: KPIItem; index: number }) {
  const trend = kpi.change_pct;
  const isUp = trend != null && trend >= 0;

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.05 }}
    >
      <Card className="hover:border-primary/30 transition-colors">
        <CardContent className="p-5">
          <p className="text-sm text-muted-foreground">{kpi.label}</p>
          <p className="mt-2 text-2xl font-bold tracking-tight">
            {formatNumber(kpi.value)}
            {kpi.unit && <span className="ml-1 text-sm font-normal text-muted-foreground">{kpi.unit}</span>}
          </p>
          {trend != null && (
            <div className={`mt-2 flex items-center gap-1 text-xs ${isUp ? "text-emerald-400" : "text-rose-400"}`}>
              {isUp ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
              {isUp ? "+" : ""}
              {trend}% vs. inicio del período
            </div>
          )}
        </CardContent>
      </Card>
    </motion.div>
  );
}
