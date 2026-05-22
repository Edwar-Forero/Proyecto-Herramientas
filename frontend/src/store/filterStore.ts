import { create } from "zustand";

interface FilterState {
  yearFrom: number;
  yearTo: number;
  department: string | null;
  setYearFrom: (y: number) => void;
  setYearTo: (y: number) => void;
  setDepartment: (d: string | null) => void;
  params: { year_from: number; year_to: number; department?: string };
}

export const useFilterStore = create<FilterState>((set, get) => ({
  yearFrom: 2020,
  yearTo: 2024,
  department: null,
  params: { year_from: 2020, year_to: 2024 },
  setYearFrom: (yearFrom) => set((state) => {
    const params = { year_from: yearFrom, year_to: state.yearTo, ...(state.department ? { department: state.department } : {}) };
    return { yearFrom, params };
  }),
  setYearTo: (yearTo) => set((state) => {
    const params = { year_from: state.yearFrom, year_to: yearTo, ...(state.department ? { department: state.department } : {}) };
    return { yearTo, params };
  }),
  setDepartment: (department) => set((state) => {
    const params = { year_from: state.yearFrom, year_to: state.yearTo, ...(department ? { department } : {}) };
    return { department, params };
  }),
}));
