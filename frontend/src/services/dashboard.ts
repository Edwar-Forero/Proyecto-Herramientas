import { api } from "./api";
import type {
  CategoryPoint,
  ComparisonItem,
  DepartmentPoint,
  KPIItem,
  OverviewResponse,
  PredictionsResponse,
  SeriesPoint,
  SystemStatus,
  User,
} from "@/types/api";

export interface FilterParams {
  year_from?: number;
  year_to?: number;
  department?: string;
}

function filterQuery(params?: FilterParams) {
  const q = new URLSearchParams();
  if (params?.year_from) q.set("year_from", String(params.year_from));
  if (params?.year_to) q.set("year_to", String(params.year_to));
  if (params?.department) q.set("department", params.department);
  const s = q.toString();
  return s ? `?${s}` : "";
}

export async function getKPIs(params?: FilterParams): Promise<{ kpis: KPIItem[]; period: string }> {
  const { data } = await api.get(`/dashboard/kpis${filterQuery(params)}`);
  return data;
}

export async function getOverview(params?: FilterParams): Promise<OverviewResponse> {
  const { data } = await api.get(`/dashboard/overview${filterQuery(params)}`);
  return data;
}

export async function getNatalidadByYear(params?: FilterParams): Promise<SeriesPoint[]> {
  const { data } = await api.get(`/natalidad/by-year${filterQuery(params)}`);
  return data.data;
}

export async function getNatalidadByDepartment(params?: FilterParams): Promise<DepartmentPoint[]> {
  const { data } = await api.get(`/natalidad/by-department${filterQuery(params)}`);
  return data.data;
}

export async function getNatalidadByGender(params?: FilterParams): Promise<CategoryPoint[]> {
  const { data } = await api.get(`/natalidad/by-gender${filterQuery(params)}`);
  return data.data;
}

export async function getNatalidadEducation(params?: FilterParams): Promise<CategoryPoint[]> {
  const { data } = await api.get(`/natalidad/by-education${filterQuery(params)}`);
  return data;
}

export async function getMortalidadByYear(params?: FilterParams): Promise<{
  fetal: SeriesPoint[];
  no_fetal: SeriesPoint[];
}> {
  const { data } = await api.get(`/mortalidad/by-year${filterQuery(params)}`);
  return data;
}

export async function getMortalidadByRegion(params?: FilterParams) {
  const { data } = await api.get(`/mortalidad/by-region${filterQuery(params)}`);
  return data;
}

export async function getNatalidadHealthRegime(params?: FilterParams): Promise<CategoryPoint[]> {
  const { data } = await api.get(`/natalidad/by-health-regime${filterQuery(params)}`);
  return data;
}

export async function getNatalidadMaternalAge(params?: FilterParams): Promise<CategoryPoint[]> {
  const { data } = await api.get(`/natalidad/by-maternal-age${filterQuery(params)}`);
  return data;
}


export async function getComparison(params?: FilterParams): Promise<ComparisonItem[]> {
  const { data } = await api.get(`/analytics/comparison${filterQuery(params)}`);
  return data.comparisons;
}

export async function getPredictions(params?: FilterParams): Promise<PredictionsResponse> {
  const { data } = await api.get(`/analytics/predictions${filterQuery(params)}`);
  return data as PredictionsResponse;
}

export async function listUsers(): Promise<User[]> {
  const { data } = await api.get("/users/list");
  return data.users;
}

export async function createUser(payload: {
  username: string;
  password: string;
  rol: string;
}): Promise<User> {
  const { data } = await api.post("/users/create", payload);
  return data;
}

export async function getSystemStatus(): Promise<SystemStatus> {
  const { data } = await api.get("/dashboard/system-status");
  return data;
}

export async function uploadDataset(file: File, collection: string, replace = false) {
  const form = new FormData();
  form.append("file", file);
  form.append("collection", collection);
  form.append("replace", String(replace));
  const { data } = await api.post("/upload/dataset", form, {
    headers: { "Content-Type": "multipart/form-data" },
  });
  return data;
}
