import Link from "next/link";
import { motion } from "framer-motion";
import { Activity, ArrowRight, BarChart3, Database, Shield } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { formatNumber } from "@/lib/utils";

const STATS = [
  { label: "Nacimientos", value: 2742429 },
  { label: "Defunciones no fetales", value: 1476123 },
  { label: "Defunciones fetales", value: 134975 },
  { label: "Período analizado", value: "2020–2024" as unknown as number },
];

const TECH = ["Next.js 15", "FastAPI", "MongoDB Atlas", "scikit-learn", "Recharts", "TailwindCSS"];

export default function LandingPage() {
  return (
    <div className="min-h-screen overflow-hidden">
      <div className="absolute inset-0 bg-hero-glow pointer-events-none" />
      <header className="relative z-10 mx-auto flex max-w-6xl items-center justify-between px-6 py-6">
        <div className="flex items-center gap-2">
          <Activity className="h-7 w-7 text-primary" />
          <span className="font-semibold">Estadísticas Vitales Colombia</span>
        </div>
        <Link href="/login">
          <Button>
            Iniciar sesión
            <ArrowRight className="h-4 w-4" />
          </Button>
        </Link>
      </header>

      <main className="relative z-10 mx-auto max-w-6xl px-6 pb-20 pt-12">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          className="text-center"
        >
          <p className="mb-4 text-sm font-medium text-primary">Business Intelligence · DANE</p>
          <h1 className="text-4xl font-bold tracking-tight sm:text-6xl">
            Análisis gerencial de
            <span className="text-gradient block sm:inline"> eventos vitales</span>
          </h1>
          <p className="mx-auto mt-6 max-w-2xl text-lg text-muted-foreground">
            Plataforma de analítica para nacimientos y defunciones en Colombia (2020–2024). Dashboards
            interactivos, roles de acceso y predicciones con machine learning.
          </p>
          <div className="mt-8 flex flex-wrap justify-center gap-4">
            <Link href="/login">
              <Button size="lg">Acceder al dashboard</Button>
            </Link>
            <Link href="/login">
              <Button size="lg" variant="outline">
                Ver demo
              </Button>
            </Link>
          </div>
        </motion.div>

        <div className="mt-16 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {STATS.map((s, i) => (
            <motion.div
              key={s.label}
              initial={{ opacity: 0, y: 16 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 * i }}
            >
              <Card>
                <CardContent className="p-5 text-center">
                  <p className="text-sm text-muted-foreground">{s.label}</p>
                  <p className="mt-2 text-2xl font-bold">
                    {typeof s.value === "number" ? formatNumber(s.value) : s.value}
                  </p>
                </CardContent>
              </Card>
            </motion.div>
          ))}
        </div>

        <div className="mt-20 grid gap-6 md:grid-cols-3">
          {[
            { icon: BarChart3, title: "Dashboards ejecutivos", desc: "KPIs, tendencias y comparaciones territoriales en tiempo real." },
            { icon: Shield, title: "Control por roles", desc: "Administrador, analista y usuario con permisos diferenciados." },
            { icon: Database, title: "Big Data MongoDB", desc: "Agregaciones optimizadas sobre millones de registros del DANE." },
          ].map((f, i) => (
            <motion.div
              key={f.title}
              initial={{ opacity: 0, y: 16 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 + i * 0.1 }}
            >
              <Card className="h-full">
                <CardContent className="p-6">
                  <f.icon className="h-8 w-8 text-primary mb-4" />
                  <h3 className="font-semibold">{f.title}</h3>
                  <p className="mt-2 text-sm text-muted-foreground">{f.desc}</p>
                </CardContent>
              </Card>
            </motion.div>
          ))}
        </div>

        <div className="mt-16 text-center">
          <p className="text-sm text-muted-foreground mb-4">Stack tecnológico</p>
          <div className="flex flex-wrap justify-center gap-2">
            {TECH.map((t) => (
              <span key={t} className="rounded-full border border-border px-3 py-1 text-xs">
                {t}
              </span>
            ))}
          </div>
        </div>
      </main>
    </div>
  );
}
