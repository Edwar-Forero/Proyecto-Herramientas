import { useEffect } from "react";
import { useRouter } from "next/router";
import { useAuthStore } from "@/store/authStore";
import type { Role } from "@/types/api";

export function useProtectedRoute(allowedRoles?: Role[]) {
  const router = useRouter();
  const { user, isAuthenticated, isLoading, loadUser } = useAuthStore();

  useEffect(() => {
    loadUser();
  }, [loadUser]);

  useEffect(() => {
    if (isLoading) return;
    if (!isAuthenticated || !user) {
      router.replace("/login");
      return;
    }
    if (allowedRoles && !allowedRoles.includes(user.rol)) {
      router.replace("/dashboard");
    }
  }, [isAuthenticated, isLoading, user, allowedRoles, router]);

  return { user, isLoading, isAuthenticated };
}
