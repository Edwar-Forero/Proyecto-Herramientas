import { useState } from "react";
import { cn } from "@/lib/utils";
import { Sidebar } from "@/components/dashboard/Sidebar";
import { Navbar } from "@/components/dashboard/Navbar";
import { useProtectedRoute } from "@/hooks/useProtectedRoute";
import { Skeleton } from "@/components/ui/skeleton";

interface Props {
  children: React.ReactNode;
  title: string;
  subtitle?: string;
  allowedRoles?: ("admin" | "analista" | "consulta")[];
}

export function DashboardLayout({ children, title, subtitle, allowedRoles }: Props) {
  const [collapsed, setCollapsed] = useState(false);
  const { isLoading } = useProtectedRoute(allowedRoles);

  if (isLoading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background p-8">
        <div className="w-full max-w-md space-y-4">
          <Skeleton className="h-8 w-3/4" />
          <Skeleton className="h-4 w-1/2" />
          <Skeleton className="h-48 w-full" />
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      <Sidebar collapsed={collapsed} onToggle={() => setCollapsed(!collapsed)} />
      <div className={cn("transition-all duration-300", collapsed ? "ml-[72px]" : "ml-64")}>
        <Navbar title={title} subtitle={subtitle} />
        <main className="p-6 animate-fade-in">{children}</main>
      </div>
    </div>
  );
}
