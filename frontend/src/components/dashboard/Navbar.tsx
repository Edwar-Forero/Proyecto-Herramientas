import { Bell, LogOut } from "lucide-react";
import { Button } from "@/components/ui/button";
import { GlobalFilters } from "./GlobalFilters";
import { useAuthStore } from "@/store/authStore";
import { Badge } from "@/components/ui/badge";

interface Props {
  title: string;
  subtitle?: string;
}

export function Navbar({ title, subtitle }: Props) {
  const { user, logout } = useAuthStore();

  return (
    <header className="sticky top-0 z-30 border-b border-border bg-background/80 backdrop-blur-xl">
      <div className="flex flex-col gap-4 px-6 py-4 lg:flex-row lg:items-center lg:justify-between">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
          {subtitle && <p className="text-sm text-muted-foreground">{subtitle}</p>}
        </div>
        <div className="flex flex-wrap items-center gap-4">
          <GlobalFilters />
          {user && <Badge className="hidden sm:inline-flex">{user.rol_label}</Badge>}
          <Button variant="ghost" size="icon" type="button" aria-label="Notificaciones">
            <Bell className="h-4 w-4" />
          </Button>
          <Button variant="outline" size="sm" onClick={logout} type="button">
            <LogOut className="h-4 w-4 mr-2" />
            Salir
          </Button>
        </div>
      </div>
    </header>
  );
}
