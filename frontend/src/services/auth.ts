import { api, tokenStorage } from "./api";
import type { TokenResponse, User } from "@/types/api";

export async function login(username: string, password: string): Promise<User> {
  const { data } = await api.post<TokenResponse>("/auth/login", { username, password });
  tokenStorage.setTokens(data.access_token, data.refresh_token);
  return fetchMe();
}

export async function register(username: string, password: string, rol = "consulta"): Promise<User> {
  const { data } = await api.post<User>("/auth/register", { username, password, rol });
  return data;
}

export async function fetchMe(): Promise<User> {
  const { data } = await api.get<User>("/auth/me");
  return data;
}

export function logout() {
  tokenStorage.clear();
  if (typeof window !== "undefined") window.location.href = "/login";
}
