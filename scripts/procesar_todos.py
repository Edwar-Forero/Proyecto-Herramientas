"""
procesar_todos.py
=================
Script unificado que procesa los 5 a\u00f1os (2020-2024) para los 3 datasets:
- Nacimientos
- Muertes Fetales
- Muertes No Fetales

Genera un Parquet por año/tipo y luego consolida cada tipo en un único Parquet.

Uso:
    python scripts/procesar_todos.py
"""

import os
import sys
import io
import pandas as pd

# Forzar UTF-8 en stdout/stderr para Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Agregar la carpeta raiz al path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from etl.cleaning import encontrar_duplicados, eliminar_duplicados, eliminar_columnas
from etl.nulls import analizar_nulos, manejar_nulos
from etl.normalization import estandarizar_nombres_columnas, ajustar_tipos_datos

# ─────────────────────── CONFIGURACIÓN ─────────────────────────
RAW = os.path.join(ROOT, "data", "raw")
OUT_NAC = os.path.join(ROOT, "data", "processed", "nacimientos")
OUT_FET = os.path.join(ROOT, "data", "processed", "fetales")
OUT_NOFET = os.path.join(ROOT, "data", "processed", "no_fetales")

for d in [OUT_NAC, OUT_FET, OUT_NOFET]:
    os.makedirs(d, exist_ok=True)

YEARS = [2020, 2021, 2022, 2023, 2024]

# ──────── Columnas a ELIMINAR (unión de colEliminar.md + extras por año) ────────

# Nacimientos: columnas que se eliminan en cualquier año que las tenga
COLS_DROP_NAC = [
    "OTRO_SIT", "APGAR1", "APGAR2", "IDPERTET", "ULTCURMAD",
    "N_HIJOSV", "FECHA_NACM", "N_EMB", "ULTCURPAD",
    # Columnas inconsistentes entre años → eliminar para unificar
    "TIPOFORMULARIO", "T_GES_AGRU_CIE", "CODPAISNACMAD",
]

# Fetales: columnas que se eliminan
COLS_DROP_FET = [
    "OTRSITIODE", "HORA", "MINUTOS", "CODPRES", "P_PMAN_IRIS",
    "CONS_EXP", "ULTCURMAD", "CODOCUR", "CODMUNOC",
    "C_MUERTE", "C_MUERTEB", "C_MUERTEC", "C_MUERTED", "C_MUERTEE",
    "C_MUERTEF", "C_MUERTEG",
    "CAUSA_MULT", "C_BAS1", "CAUSA_667", "IDPROFCER",
    # Columnas inconsistentes
    "T_GES_AGRU_CIE", "TIPOFORMULARIO",
    "CODPAISNACMAD", "REGSOCIALMADRE",
]

# No Fetales: columnas que se eliminan
COLS_DROP_NOFET = [
    "OTRSITIODE", "HORA", "MINUTOS", "CODPRES", "P_PMAN_IRIS",
    "CONS_EXP", "ULTCURMAD", "CODOCUR", "CODMUNOC",
    "C_MUERTE", "C_MUERTEB", "C_MUERTEC", "C_MUERTED", "C_MUERTEE",
    "C_MUERTEF", "C_MUERTEG",
    "CAUSA_MULT", "C_BAS1", "CAUSA_667", "IDPROFCER",
    "MU_PARTO", "CAU_HOMOL",
    # Columnas inconsistentes
    "T_GES_AGRU_CIE", "TIPOFORMULARIO",
    "CODPAISNACMAD", "CODPAISNACFAL", "REGSOCIALMADRE",
]

# Columnas con >90% nulos en no-fetales (solo aplican a menores de edad/partos)
COLS_ALTO_NULO_NOFET = [
    "simuertepo", "t_ges", "niv_edum", "n_hijosm", "tipo_emb",
    "t_parto", "est_civm", "peso_nac", "edad_madre", "n_hijosv",
    "emb_mes", "emb_sem", "emb_fal",
]

# ──────── Columnas objetivo finales (tras estandarizar a minúsculas) ────────

FINAL_COLS_NAC = [
    "cod_dpto", "cod_munic", "areanac", "sit_parto", "sexo",
    "peso_nac", "talla_nac", "ano", "mes", "aten_par", "t_ges",
    "numconsul", "tipo_parto", "mul_parto", "idhemoclas", "idfactorrh",
    "edad_madre", "est_civm", "niv_edum", "codpres", "codptore",
    "codmunre", "area_res", "seg_social", "idclasadmi", "edad_padre",
    "niv_edup", "profesion",
]

FINAL_COLS_FET = [
    "cod_dpto", "cod_munic", "a_defun", "sit_defun", "tipo_defun",
    "ano", "mes", "sexo", "codptore", "codmunre", "area_res",
    "seg_social", "idadmisalud", "mu_parto", "t_parto", "tipo_emb",
    "t_ges", "peso_nac", "edad_madre", "n_hijosv", "n_hijosm",
    "est_civm", "niv_edum", "asis_med", "cau_homol",
]

FINAL_COLS_NOFET = [
    "cod_dpto", "cod_munic", "a_defun", "sit_defun", "tipo_defun",
    "ano", "mes", "sexo", "est_civil", "gru_ed1", "gru_ed2",
    "nivel_edu", "ultcurfal", "muerteporo", "ocupacion", "idpertet",
    "codptore", "codmunre", "area_res", "seg_social", "idadmisalud",
    "asis_med",
]


# ═══════════════════════════════════════════════════════════════════
#  FUNCIONES DE PROCESAMIENTO
# ═══════════════════════════════════════════════════════════════════

def load_raw(prefix: str, year: int) -> pd.DataFrame:
    """Carga un CSV crudo."""
    path = os.path.join(RAW, f"{prefix}{year}.csv")
    print(f"\n{'='*60}")
    print(f"  Cargando {path}")
    print(f"{'='*60}")
    df = pd.read_csv(path, sep=",", encoding="latin-1", low_memory=False)
    print(f"  → {len(df)} registros, {len(df.columns)} columnas")
    return df


def proceso_comun(df: pd.DataFrame, cols_drop: list, year: int, tipo: str) -> pd.DataFrame:
    """
    Pipeline común: duplicados → eliminar columnas → estandarizar nombres.
    """
    print(f"\n--- [{year}] {tipo}: Eliminación de duplicados ---")
    encontrar_duplicados(df)
    df = eliminar_duplicados(df)

    print(f"\n--- [{year}] {tipo}: Eliminación de columnas ---")
    df = eliminar_columnas(df, cols_drop)

    print(f"\n--- [{year}] {tipo}: Estandarización de nombres ---")
    df = estandarizar_nombres_columnas(df)

    return df


# ────────────────── NACIMIENTOS ──────────────────

def procesar_nacimientos(year: int) -> pd.DataFrame:
    df = load_raw("nac", year)
    df = proceso_comun(df, COLS_DROP_NAC, year, "Nacimientos")

    # Analizar nulos
    print(f"\n--- [{year}] Nacimientos: Análisis de nulos ---")
    analizar_nulos(df)

    # Eliminar nulos geográficos (codptore, codmunre, area_res)
    geo_cols = ["codptore", "codmunre", "area_res"]
    geo_present = [c for c in geo_cols if c in df.columns]
    if geo_present:
        antes = len(df)
        df = df.dropna(subset=geo_present)
        print(f"  Eliminados {antes - len(df)} registros con nulos geográficos")

    # Imputar idclasadmi
    estrategia = {}
    if "idclasadmi" in df.columns:
        estrategia["idclasadmi"] = {"metodo": "fill", "valor": "DESCONOCIDO"}
    # Imputar profesion si tiene nulos (aparece en 2023+)
    if "profesion" in df.columns and df["profesion"].isnull().sum() > 0:
        estrategia["profesion"] = {"metodo": "fill_mode"}
    # Imputar codpres si tiene nulos
    if "codpres" in df.columns and df["codpres"].isnull().sum() > 0:
        estrategia["codpres"] = {"metodo": "fill_mode"}

    if estrategia:
        df = manejar_nulos(df, estrategia)

    # Ajustar tipos
    df = ajustar_tipos_datos(df)

    # Seleccionar solo columnas finales que existan
    cols_presentes = [c for c in FINAL_COLS_NAC if c in df.columns]
    df = df[cols_presentes]

    print(f"\n  ✓ Nacimientos {year}: {len(df)} registros, {len(df.columns)} columnas finales")
    return df


# ────────────────── FETALES ──────────────────

def procesar_fetales(year: int) -> pd.DataFrame:
    df = load_raw("fetal", year)
    
    # Para 2024: SEG_SOCIAL no existe, pero puede haber REGSOCIALMADRE
    # Hay que renombrar antes de eliminar
    if "REGSOCIALMADRE" in df.columns and "SEG_SOCIAL" not in df.columns:
        df = df.rename(columns={"REGSOCIALMADRE": "SEG_SOCIAL"})
        print(f"  [{year}] Renombrada REGSOCIALMADRE → SEG_SOCIAL")

    df = proceso_comun(df, COLS_DROP_FET, year, "Fetales")

    # Analizar nulos
    print(f"\n--- [{year}] Fetales: Análisis de nulos ---")
    analizar_nulos(df)

    # Eliminar nulos geográficos
    geo_cols = ["codptore", "codmunre", "area_res"]
    geo_present = [c for c in geo_cols if c in df.columns]
    if geo_present:
        antes = len(df)
        df = df.dropna(subset=geo_present)
        print(f"  Eliminados {antes - len(df)} registros con nulos geográficos")

    # Imputar idadmisalud
    estrategia = {}
    if "idadmisalud" in df.columns and df["idadmisalud"].isnull().sum() > 0:
        estrategia["idadmisalud"] = {"metodo": "fill", "valor": "DESCONOCIDO"}

    if estrategia:
        df = manejar_nulos(df, estrategia)

    # Ajustar tipos
    df = ajustar_tipos_datos(df)

    # Seleccionar columnas finales (crear faltantes con valor por defecto)
    for c in FINAL_COLS_FET:
        if c not in df.columns:
            if c == "idadmisalud":
                df[c] = "DESCONOCIDO"
                print(f"  [{year}] Columna '{c}' no existe, creada con 'DESCONOCIDO'")
            else:
                df[c] = pd.NA
                print(f"  [{year}] Columna '{c}' no existe, creada con NA")
    cols_presentes = [c for c in FINAL_COLS_FET if c in df.columns]
    df = df[cols_presentes]

    print(f"\n  ✓ Fetales {year}: {len(df)} registros, {len(df.columns)} columnas finales")
    return df


# ────────────────── NO FETALES ──────────────────

def procesar_no_fetales(year: int) -> pd.DataFrame:
    df = load_raw("nofetal", year)

    # Para 2024: renombrar columnas que cambian de nombre
    if "REGSOCIALMADRE" in df.columns and "SEG_SOCIAL" not in df.columns:
        df = df.rename(columns={"REGSOCIALMADRE": "SEG_SOCIAL"})
        print(f"  [{year}] Renombrada REGSOCIALMADRE → SEG_SOCIAL")

    df = proceso_comun(df, COLS_DROP_NOFET, year, "No Fetales")

    # Analizar nulos
    print(f"\n--- [{year}] No Fetales: Análisis de nulos ---")
    analizar_nulos(df)

    # Eliminar nulos geográficos
    geo_cols = ["codptore", "codmunre", "area_res"]
    geo_present = [c for c in geo_cols if c in df.columns]
    if geo_present:
        antes = len(df)
        df = df.dropna(subset=geo_present)
        print(f"  Eliminados {antes - len(df)} registros con nulos geográficos")

    # Eliminar columnas con >90% nulos (datos de parto solo para menores)
    cols_alto_nulo = [c for c in COLS_ALTO_NULO_NOFET if c in df.columns]
    if cols_alto_nulo:
        df = df.drop(columns=cols_alto_nulo)
        print(f"  Eliminadas {len(cols_alto_nulo)} columnas con >90% nulos")

    # Imputar
    estrategia = {}
    if "ocupacion" in df.columns and df["ocupacion"].isnull().sum() > 0:
        estrategia["ocupacion"] = {"metodo": "fill", "valor": "DESCONOCIDO"}
    if "muerteporo" in df.columns and df["muerteporo"].isnull().sum() > 0:
        estrategia["muerteporo"] = {"metodo": "fill_mode"}
    if "idadmisalud" in df.columns and df["idadmisalud"].isnull().sum() > 0:
        estrategia["idadmisalud"] = {"metodo": "fill", "valor": "DESCONOCIDO"}

    if estrategia:
        df = manejar_nulos(df, estrategia)

    # Ajustar tipos
    df = ajustar_tipos_datos(df)

    # Seleccionar columnas finales (crear faltantes con valor por defecto)
    for c in FINAL_COLS_NOFET:
        if c not in df.columns:
            if c == "idadmisalud":
                df[c] = "DESCONOCIDO"
                print(f"  [{year}] Columna '{c}' no existe, creada con 'DESCONOCIDO'")
            else:
                df[c] = pd.NA
                print(f"  [{year}] Columna '{c}' no existe, creada con NA")
    cols_presentes = [c for c in FINAL_COLS_NOFET if c in df.columns]
    df = df[cols_presentes]

    print(f"\n  ✓ No Fetales {year}: {len(df)} registros, {len(df.columns)} columnas finales")
    return df


# ═══════════════════════════════════════════════════════════════════
#  CONSOLIDACIÓN
# ═══════════════════════════════════════════════════════════════════

def consolidar(dfs: list[pd.DataFrame], nombre: str, out_dir: str):
    """Concatena DataFrames y guarda el dataset consolidado."""
    print(f"\n{'='*60}")
    print(f"  CONSOLIDANDO: {nombre.upper()}")
    print(f"{'='*60}")

    df_all = pd.concat(dfs, ignore_index=True)

    print(f"  Total registros consolidados: {len(df_all)}")
    print(f"  Columnas: {list(df_all.columns)}")
    print(f"  Años presentes: {sorted(df_all['ano'].unique())}")

    # Verificar nulos residuales
    nulos_res = df_all.isnull().sum()
    nulos_res = nulos_res[nulos_res > 0]
    if len(nulos_res) > 0:
        print(f"\n  ⚠ Nulos residuales:")
        for col, n in nulos_res.items():
            pct = n / len(df_all) * 100
            print(f"    {col}: {n} ({pct:.2f}%)")

    # Guardar
    out_path = os.path.join(out_dir, f"{nombre}_consolidado.parquet")
    df_all.to_parquet(out_path, index=False)
    print(f"\n  ✓ Guardado en: {out_path}")
    print(f"    Tamaño: {os.path.getsize(out_path) / 1024 / 1024:.1f} MB")

    return df_all


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  ETL Pipeline - Estadisticas Vitales Colombia 2020-2024")
    print("=" * 60)

    dfs_nac = []
    dfs_fet = []
    dfs_nofet = []

    for year in YEARS:
        # ── Nacimientos ──
        df_nac = procesar_nacimientos(year)
        out = os.path.join(OUT_NAC, f"nacimientos_{year}.parquet")
        df_nac.to_parquet(out, index=False)
        print(f"  [OK] Guardado: {out}")
        dfs_nac.append(df_nac)

        # ── Fetales ──
        df_fet = procesar_fetales(year)
        out = os.path.join(OUT_FET, f"fetales_{year}.parquet")
        df_fet.to_parquet(out, index=False)
        print(f"  [OK] Guardado: {out}")
        dfs_fet.append(df_fet)

        # ── No Fetales ──
        df_nofet = procesar_no_fetales(year)
        out = os.path.join(OUT_NOFET, f"no_fetales_{year}.parquet")
        df_nofet.to_parquet(out, index=False)
        print(f"  [OK] Guardado: {out}")
        dfs_nofet.append(df_nofet)

    # ── Consolidar ──
    consolidar(dfs_nac, "nacimientos", OUT_NAC)
    consolidar(dfs_fet, "fetales", OUT_FET)
    consolidar(dfs_nofet, "no_fetales", OUT_NOFET)

    print("\n" + "="*60)
    print("  PIPELINE COMPLETO")
    print("="*60)


if __name__ == "__main__":
    main()
