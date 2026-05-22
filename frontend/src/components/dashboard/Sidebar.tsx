import Link from "next/link";
import { useRouter } from "next/router";
import {
  Activity,
  Baby,
  BarChart3,
  ChevronLeft,
  ChevronRight,
  HeartPulse,
  LayoutDashboard,
  Map,
  Settings,
  Sparkles,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/store/authStore";
import type { Role } from "@/types/api";

interface NavItem {
  href: string;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  roles: Role[];
}

const NAV: NavItem[] = [
  { href: "/dashboard", label: "Overview", icon: LayoutDashboard, roles: ["admin", "analista", "consulta"] },
  { href: "/dashboard/natalidad", label: "Natalidad", icon: Baby, roles: ["admin", "analista", "consulta"] },
  { href: "/dashboard/mortalidad", label: "Mortalidad", icon: HeartPulse, roles: ["admin", "analista", "consulta"] },
  { href: "/dashboard/territorial", label: "Territorial", icon: Map, roles: ["admin", "analista", "consulta"] },
  { href: "/dashboard/predicciones", label: "Predicciones", icon: Sparkles, roles: ["admin", "analista"] },
  { href: "/dashboard/analisis", label: "Análisis", icon: BarChart3, roles: ["admin", "analista"] },
  { href: "/dashboard/admin", label: "Administración", icon: Settings, roles: ["admin"] },
];

interface Props {
  collapsed: boolean;
  onToggle: () => void;
}

export function Sidebar({ collapsed, onToggle }: Props) {
  const router = useRouter();
  const { user, hasRole } = useAuthStore();

  const items = NAV.filter((item) => hasRole(...item.roles));

  return (
    <aside
      className={cn(
        "fixed left-0 top-0 z-40 flex h-screen flex-col border-r border-border bg-card/80 backdrop-blur-xl transition-all duration-300",
        collapsed ? "w-[72px]" : "w-64"
      )}
    >
      <div className="flex h-16 items-center gap-2 border-b border-border px-4">
        <Activity className="h-6 w-6 text-primary shrink-0" />
        {!collapsed && (
          <div>
            <p className="text-sm font-semibold">Vitales CO</p>
            <p className="text-xs text-muted-foreground">BI Dashboard</p>
          </div>
        )}
      </div>

      <nav className="flex-1 space-y-1 p-3">
        {items.map((item) => {
          const active = router.pathname === item.href;
          const Icon = item.icon;
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm transition-colors",
                active ? "bg-primary/15 text-primary" : "text-muted-foreground hover:bg-accent hover:text-foreground"
              )}
              title={collapsed ? item.label : undefined}
            >
              <Icon className="h-5 w-5 shrink-0" />
              {!collapsed && <span>{item.label}</span>}
            </Link>
          );
        })}
      </nav>

      <div className="border-t border-border p-3">
        {!collapsed && user && (
          <div className="mb-2 px-2">
            <p className="text-xs text-muted-foreground">Sesión</p>
            <p className="text-sm font-medium truncate">{user.username}</p>
            <p className="text-xs text-primary">{user.rol_label}</p>
          </div>
        )}
        <button
          onClick={onToggle}
          className="flex w-full items-center justify-center rounded-lg p-2 text-muted-foreground hover:bg-accent"
          type="button"
        >
          {collapsed ? <ChevronRight className="h-5 w-5" /> : <ChevronLeft className="h-5 w-5" />}
        </button>
      </div>
    </aside>
  );
}
