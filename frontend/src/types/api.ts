export type Role = "admin" | "analista" | "consulta";

export interface User {
  id: string;
  username: string;
  rol: Role;
  rol_label: string;
  activo: boolean;
}

export interface TokenResponse {
  access_token: string;
  refresh_token: string;
  token_type: string;
}

export interface KPIItem {
  label: string;
  value: number;
  change_pct?: number | null;
  unit?: string | null;
}

export interface SeriesPoint {
  year: number;
  value: number;
  label?: string | null;
}

export interface CategoryPoint {
  category: string;
  value: number;
  percentage?: number | null;
}

export interface DepartmentPoint {
  department: string;
  value: number;
  rank?: number | null;
}

export interface OverviewResponse {
  kpis: KPIItem[];
  births_by_year: SeriesPoint[];
  deaths_by_year: SeriesPoint[];
  top_departments: DepartmentPoint[];
}

export interface ComparisonItem {
  metric: string;
  natalidad: number;
  mortalidad_fetal: number;
  mortalidad_no_fetal: number;
}

export interface PredictionPoint {
  year: number;
  predicted: number;
  metric: string;
  confidence?: number | null;
}

export interface SystemStatus {
  database: string;
  collections: Record<string, number>;
  total_records: number;
  status: string;
}
