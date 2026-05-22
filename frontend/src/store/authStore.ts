import { create } from "zustand";
import { persist } from "zustand/middleware";
import { fetchMe, logout as authLogout } from "@/services/auth";
import { tokenStorage } from "@/services/api";
import type { Role, User } from "@/types/api";

interface AuthState {
  user: User | null;
  isLoading: boolean;
  isAuthenticated: boolean;
  setUser: (user: User | null) => void;
  loadUser: () => Promise<void>;
  logout: () => void;
  hasRole: (...roles: Role[]) => boolean;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => ({
      user: null,
      isLoading: false,
      isAuthenticated: false,

      setUser: (user) => set({ user, isAuthenticated: !!user }),

      loadUser: async () => {
        if (!tokenStorage.getAccess()) {
          set({ user: null, isAuthenticated: false });
          return;
        }
        set({ isLoading: true });
        try {
          const user = await fetchMe();
          set({ user, isAuthenticated: true, isLoading: false });
        } catch {
          tokenStorage.clear();
          set({ user: null, isAuthenticated: false, isLoading: false });
        }
      },

      logout: () => {
        authLogout();
        set({ user: null, isAuthenticated: false });
      },

      hasRole: (...roles) => {
        const rol = get().user?.rol;
        return !!rol && roles.includes(rol);
      },
    }),
    { name: "ev-auth", partialize: (s) => ({ user: s.user, isAuthenticated: s.isAuthenticated }) }
  )
);
