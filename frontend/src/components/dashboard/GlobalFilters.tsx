import { useFilterStore } from "@/store/filterStore";
import { Label } from "@/components/ui/label";

const YEARS = [2020, 2021, 2022, 2023, 2024];

export function GlobalFilters() {
  const { yearFrom, yearTo, setYearFrom, setYearTo } = useFilterStore();

  return (
    <div className="flex flex-wrap items-end gap-4">
      <div>
        <Label htmlFor="year-from">Desde</Label>
        <select
          id="year-from"
          value={yearFrom}
          onChange={(e) => setYearFrom(Number(e.target.value))}
          className="mt-1 h-9 rounded-lg border border-border bg-muted/50 px-3 text-sm"
        >
          {YEARS.map((y) => (
            <option key={y} value={y}>
              {y}
            </option>
          ))}
        </select>
      </div>
      <div>
        <Label htmlFor="year-to">Hasta</Label>
        <select
          id="year-to"
          value={yearTo}
          onChange={(e) => setYearTo(Number(e.target.value))}
          className="mt-1 h-9 rounded-lg border border-border bg-muted/50 px-3 text-sm"
        >
          {YEARS.map((y) => (
            <option key={y} value={y}>
              {y}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}
