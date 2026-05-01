"""
analisis_exploratorio.py
========================
Análisis exploratorio de datos (EDA) sobre los 3 datasets consolidados:
- Nacimientos
- Muertes Fetales
- Muertes No Fetales

Genera estadísticas descriptivas, correlaciones, tendencias temporales
y guarda las visualizaciones en data/processed/graficos/.

Uso:
    python scripts/analisis_exploratorio.py
"""

import os
import sys
import io
import warnings
import pandas as pd
import numpy as np
import matplotlib

# Forzar UTF-8 en stdout/stderr para Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
matplotlib.use("Agg")  # Backend sin GUI para generar imágenes
import matplotlib.pyplot as plt
import seaborn as sns

warnings.filterwarnings("ignore")

# ─── Rutas ───
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

PROCESSED = os.path.join(ROOT, "data", "processed")
GRAFICOS = os.path.join(PROCESSED, "graficos")
os.makedirs(GRAFICOS, exist_ok=True)

# ─── Estilo de gráficos ───
plt.rcParams.update({
    "figure.figsize": (14, 8),
    "font.size": 12,
    "axes.titlesize": 16,
    "axes.labelsize": 13,
    "figure.dpi": 150,
    "savefig.dpi": 150,
})
sns.set_theme(style="whitegrid", palette="deep")


# ═══════════════════════════════════════════════════════════════════
#  CARGA DE DATOS
# ═══════════════════════════════════════════════════════════════════

def cargar_datos():
    """Carga los 3 datasets consolidados."""
    datasets = {}
    for nombre in ["nacimientos", "fetales", "no_fetales"]:
        path = os.path.join(PROCESSED, nombre, f"{nombre}_consolidado.parquet")
        if os.path.exists(path):
            df = pd.read_parquet(path)
            datasets[nombre] = df
            print(f"✓ {nombre}: {len(df):,} registros, {len(df.columns)} columnas")
        else:
            print(f"✗ {nombre}: archivo no encontrado ({path})")
    return datasets


# ═══════════════════════════════════════════════════════════════════
#  1. ESTADÍSTICAS DESCRIPTIVAS
# ═══════════════════════════════════════════════════════════════════

def estadisticas_descriptivas(datasets: dict):
    """Genera un resumen estadístico de cada dataset."""
    print("\n" + "="*70)
    print("  1. ESTADÍSTICAS DESCRIPTIVAS")
    print("="*70)

    for nombre, df in datasets.items():
        print(f"\n{'─'*50}")
        print(f"  {nombre.upper()} ({len(df):,} registros)")
        print(f"{'─'*50}")

        # Conteos por año
        conteo_anual = df.groupby("ano").size().reset_index(name="registros")
        print(f"\n  Registros por año:")
        for _, row in conteo_anual.iterrows():
            print(f"    {int(row['ano'])}: {int(row['registros']):,}")

        # Info de columnas numéricas
        num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if num_cols:
            desc = df[num_cols].describe().T
            print(f"\n  Estadísticas numéricas (top 10):")
            print(desc.head(10).to_string())

    return True


# ═══════════════════════════════════════════════════════════════════
#  2. TENDENCIAS TEMPORALES
# ═══════════════════════════════════════════════════════════════════

def tendencias_temporales(datasets: dict):
    """Gráficos de tendencias anuales."""
    print("\n" + "="*70)
    print("  2. TENDENCIAS TEMPORALES")
    print("="*70)

    # ── 2a. Conteo total por año y tipo ──
    fig, ax = plt.subplots(figsize=(12, 6))
    colors = {"nacimientos": "#2196F3", "fetales": "#FF5722", "no_fetales": "#4CAF50"}
    labels_es = {"nacimientos": "Nacimientos", "fetales": "Muertes Fetales", "no_fetales": "Muertes No Fetales"}

    for nombre, df in datasets.items():
        conteo = df.groupby("ano").size()
        ax.plot(conteo.index, conteo.values, marker="o", linewidth=2.5,
                markersize=8, label=labels_es[nombre], color=colors[nombre])
        # Anotar valores
        for x, y in zip(conteo.index, conteo.values):
            ax.annotate(f"{y:,.0f}", (x, y), textcoords="offset points",
                       xytext=(0, 12), ha="center", fontsize=9)

    ax.set_title("Tendencia Anual de Eventos — Colombia 2020-2024", fontweight="bold")
    ax.set_xlabel("Año")
    ax.set_ylabel("Número de Registros")
    ax.legend(frameon=True, fontsize=11)
    ax.set_xticks([2020, 2021, 2022, 2023, 2024])
    plt.tight_layout()
    plt.savefig(os.path.join(GRAFICOS, "01_tendencia_anual.png"))
    plt.close()
    print("  ✓ 01_tendencia_anual.png")

    # ── 2b. Nacimientos por mes (heatmap) ──
    if "nacimientos" in datasets:
        df_nac = datasets["nacimientos"]
        pivot = df_nac.groupby(["ano", "mes"]).size().unstack(fill_value=0)
        fig, ax = plt.subplots(figsize=(14, 6))
        sns.heatmap(pivot, annot=True, fmt=",d", cmap="YlOrRd", ax=ax,
                    linewidths=0.5, cbar_kws={"label": "Nacimientos"})
        ax.set_title("Nacimientos por Mes y Año — Colombia", fontweight="bold")
        ax.set_xlabel("Mes")
        ax.set_ylabel("Año")
        plt.tight_layout()
        plt.savefig(os.path.join(GRAFICOS, "02_nacimientos_heatmap.png"))
        plt.close()
        print("  ✓ 02_nacimientos_heatmap.png")


# ═══════════════════════════════════════════════════════════════════
#  3. DISTRIBUCIÓN POR SEXO
# ═══════════════════════════════════════════════════════════════════

def distribucion_sexo(datasets: dict):
    """Análisis de distribución por sexo."""
    print("\n" + "="*70)
    print("  3. DISTRIBUCIÓN POR SEXO")
    print("="*70)

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    labels_es = {"nacimientos": "Nacimientos", "fetales": "Muertes Fetales", "no_fetales": "Muertes No Fetales"}
    sexo_map = {1: "Masculino", 2: "Femenino", 3: "Indeterminado"}

    for idx, (nombre, df) in enumerate(datasets.items()):
        if "sexo" in df.columns:
            conteo = df["sexo"].map(sexo_map).value_counts()
            colors = ["#42A5F5", "#EF5350", "#66BB6A"][:len(conteo)]
            conteo.plot(kind="bar", ax=axes[idx], color=colors, edgecolor="white")
            axes[idx].set_title(labels_es.get(nombre, nombre), fontweight="bold")
            axes[idx].set_ylabel("Registros")
            axes[idx].tick_params(axis="x", rotation=0)

            # Porcentajes
            total = conteo.sum()
            for i, (val, cnt) in enumerate(conteo.items()):
                axes[idx].text(i, cnt + total*0.01, f"{cnt/total*100:.1f}%",
                             ha="center", fontsize=10, fontweight="bold")

    plt.suptitle("Distribución por Sexo — 2020-2024", fontsize=18, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(GRAFICOS, "03_distribucion_sexo.png"))
    plt.close()
    print("  ✓ 03_distribucion_sexo.png")


# ═══════════════════════════════════════════════════════════════════
#  4. ANÁLISIS GEOGRÁFICO (POR DEPARTAMENTO)
# ═══════════════════════════════════════════════════════════════════

def analisis_geografico(datasets: dict):
    """Top departamentos por número de eventos."""
    print("\n" + "="*70)
    print("  4. ANÁLISIS GEOGRÁFICO")
    print("="*70)

    fig, axes = plt.subplots(1, 3, figsize=(20, 8))
    labels_es = {"nacimientos": "Nacimientos", "fetales": "Muertes Fetales", "no_fetales": "Muertes No Fetales"}
    palettes = {"nacimientos": "Blues_r", "fetales": "Reds_r", "no_fetales": "Greens_r"}

    for idx, (nombre, df) in enumerate(datasets.items()):
        if "cod_dpto" in df.columns:
            top_dptos = df["cod_dpto"].value_counts().head(15)
            sns.barplot(x=top_dptos.values, y=top_dptos.index.astype(str),
                       palette=palettes[nombre], ax=axes[idx])
            axes[idx].set_title(f"Top 15 Dptos - {labels_es[nombre]}", fontweight="bold")
            axes[idx].set_xlabel("Registros")
            axes[idx].set_ylabel("Código Departamento")

    plt.suptitle("Distribución Geográfica por Departamento — 2020-2024",
                fontsize=18, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(GRAFICOS, "04_top_departamentos.png"))
    plt.close()
    print("  ✓ 04_top_departamentos.png")


# ═══════════════════════════════════════════════════════════════════
#  5. CORRELACIONES — NACIMIENTOS
# ═══════════════════════════════════════════════════════════════════

def correlaciones_nacimientos(datasets: dict):
    """Matriz de correlación para nacimientos."""
    print("\n" + "="*70)
    print("  5. CORRELACIONES — NACIMIENTOS")
    print("="*70)

    if "nacimientos" not in datasets:
        print("  ✗ Dataset de nacimientos no disponible")
        return

    df = datasets["nacimientos"]

    # Variables de interés para correlación
    vars_corr = [
        "peso_nac", "talla_nac", "t_ges", "numconsul",
        "edad_madre", "edad_padre", "niv_edum", "niv_edup",
    ]
    vars_presentes = [v for v in vars_corr if v in df.columns]

    if len(vars_presentes) < 2:
        print("  ✗ Insuficientes variables numéricas")
        return

    df_num = df[vars_presentes].select_dtypes(include=[np.number])
    corr = df_num.corr()

    # Heatmap
    fig, ax = plt.subplots(figsize=(10, 8))
    mask = np.triu(np.ones_like(corr, dtype=bool))
    sns.heatmap(corr, mask=mask, annot=True, fmt=".2f", cmap="RdBu_r",
               center=0, vmin=-1, vmax=1, square=True, ax=ax,
               linewidths=0.5, cbar_kws={"shrink": 0.8})
    ax.set_title("Correlaciones — Variables de Nacimientos", fontweight="bold")
    plt.tight_layout()
    plt.savefig(os.path.join(GRAFICOS, "05_correlaciones_nacimientos.png"))
    plt.close()
    print("  ✓ 05_correlaciones_nacimientos.png")

    # Imprimir correlaciones significativas
    print("\n  Correlaciones significativas (|r| > 0.3):")
    for i in range(len(corr.columns)):
        for j in range(i+1, len(corr.columns)):
            r = corr.iloc[i, j]
            if abs(r) > 0.3:
                print(f"    {corr.columns[i]} ↔ {corr.columns[j]}: r = {r:.3f}")


# ═══════════════════════════════════════════════════════════════════
#  6. ANÁLISIS DE EDAD MATERNA
# ═══════════════════════════════════════════════════════════════════

def analisis_edad_materna(datasets: dict):
    """Distribución de edad materna en nacimientos y muertes fetales."""
    print("\n" + "="*70)
    print("  6. ANÁLISIS DE EDAD MATERNA")
    print("="*70)

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    for idx, (nombre, label) in enumerate([("nacimientos", "Nacimientos"), ("fetales", "Muertes Fetales")]):
        if nombre in datasets and "edad_madre" in datasets[nombre].columns:
            df = datasets[nombre]
            # Filtrar valores válidos (no 99 ni 999 que suelen ser "desconocido")
            edades = df["edad_madre"][(df["edad_madre"] > 10) & (df["edad_madre"] < 55)]

            # Histograma por año
            for year in sorted(df["ano"].unique()):
                edades_year = df[(df["ano"] == year) & (df["edad_madre"] > 10) & (df["edad_madre"] < 55)]["edad_madre"]
                axes[idx].hist(edades_year, bins=range(10, 56), alpha=0.4, label=str(int(year)))

            axes[idx].set_title(f"Edad Materna — {label}", fontweight="bold")
            axes[idx].set_xlabel("Edad")
            axes[idx].set_ylabel("Frecuencia")
            axes[idx].legend(fontsize=9)

    plt.suptitle("Distribución de Edad Materna — Colombia 2020-2024",
                fontsize=18, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(GRAFICOS, "06_edad_materna.png"))
    plt.close()
    print("  ✓ 06_edad_materna.png")

    # Estadísticas
    if "nacimientos" in datasets:
        df_nac = datasets["nacimientos"]
        edades = df_nac["edad_madre"][(df_nac["edad_madre"] > 10) & (df_nac["edad_madre"] < 55)]
        print(f"\n  Nacimientos — Edad materna:")
        print(f"    Media: {edades.mean():.1f}")
        print(f"    Mediana: {edades.median():.0f}")
        print(f"    Madres adolescentes (<18): {(edades < 18).sum():,} ({(edades < 18).mean()*100:.1f}%)")
        print(f"    Madres añosas (≥35): {(edades >= 35).sum():,} ({(edades >= 35).mean()*100:.1f}%)")


# ═══════════════════════════════════════════════════════════════════
#  7. PESO AL NACER vs EDAD GESTACIONAL
# ═══════════════════════════════════════════════════════════════════

def peso_vs_gestacion(datasets: dict):
    """Relación entre peso al nacer y tiempo de gestación."""
    print("\n" + "="*70)
    print("  7. PESO AL NACER vs EDAD GESTACIONAL")
    print("="*70)

    if "nacimientos" not in datasets:
        return

    df = datasets["nacimientos"]

    if "peso_nac" not in df.columns or "t_ges" not in df.columns:
        return

    # Filtrar valores sensibles
    mask = (df["peso_nac"] > 0) & (df["peso_nac"] < 10) & (df["t_ges"] > 0) & (df["t_ges"] < 50)
    df_fil = df[mask]

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Boxplot de peso por categoría gestacional
    # Crear categorías: prematuro (<37), a término (37-41), postérmino (>41)
    bins = [0, 28, 32, 37, 42, 50]
    labels = ["<28 sem", "28-31", "32-36", "37-41", ">41 sem"]
    df_fil = df_fil.copy()
    df_fil["cat_gest"] = pd.cut(df_fil["t_ges"], bins=bins, labels=labels)

    sns.boxplot(data=df_fil, x="cat_gest", y="peso_nac", ax=axes[0],
               palette="YlOrRd")
    axes[0].set_title("Peso al Nacer por Edad Gestacional", fontweight="bold")
    axes[0].set_xlabel("Categoría Gestacional")
    axes[0].set_ylabel("Categoría de Peso")

    # Scatter con densidad
    sample = df_fil.sample(min(50000, len(df_fil)), random_state=42)
    axes[1].hexbin(sample["t_ges"], sample["peso_nac"], gridsize=30,
                  cmap="YlOrRd", mincnt=1)
    axes[1].set_title("Densidad: Gestación vs Peso", fontweight="bold")
    axes[1].set_xlabel("Semanas de Gestación")
    axes[1].set_ylabel("Categoría de Peso al Nacer")
    plt.colorbar(axes[1].collections[0], ax=axes[1], label="Frecuencia")

    plt.suptitle("Relación Peso al Nacer — Edad Gestacional",
                fontsize=18, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(GRAFICOS, "07_peso_vs_gestacion.png"))
    plt.close()
    print("  ✓ 07_peso_vs_gestacion.png")


# ═══════════════════════════════════════════════════════════════════
#  8. TIPO DE PARTO Y SEGURIDAD SOCIAL
# ═══════════════════════════════════════════════════════════════════

def tipo_parto_seg_social(datasets: dict):
    """Análisis de tipo de parto y seguridad social."""
    print("\n" + "="*70)
    print("  8. TIPO DE PARTO Y SEGURIDAD SOCIAL")
    print("="*70)

    if "nacimientos" not in datasets:
        return

    df = datasets["nacimientos"]

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Tipo de parto por año
    if "tipo_parto" in df.columns:
        parto_map = {1: "Espontáneo", 2: "Cesárea", 3: "Instrumentado", 9: "Sin info"}
        conteo = df.groupby(["ano", "tipo_parto"]).size().unstack(fill_value=0)
        conteo.columns = [parto_map.get(c, str(c)) for c in conteo.columns]
        conteo_pct = conteo.div(conteo.sum(axis=1), axis=0) * 100
        conteo_pct.plot(kind="bar", stacked=True, ax=axes[0],
                       colormap="Set2", edgecolor="white")
        axes[0].set_title("Tipo de Parto por Año (%)", fontweight="bold")
        axes[0].set_ylabel("Porcentaje")
        axes[0].legend(fontsize=9, loc="upper right")
        axes[0].tick_params(axis="x", rotation=0)

    # Seguridad social por año
    if "seg_social" in df.columns:
        seg_map = {1: "Contributivo", 2: "Subsidiado", 3: "Excepción",
                   4: "Especial", 5: "No asegurado", 9: "Sin info"}
        conteo_seg = df.groupby(["ano", "seg_social"]).size().unstack(fill_value=0)
        conteo_seg.columns = [seg_map.get(c, str(c)) for c in conteo_seg.columns]
        conteo_seg_pct = conteo_seg.div(conteo_seg.sum(axis=1), axis=0) * 100
        conteo_seg_pct.plot(kind="bar", stacked=True, ax=axes[1],
                           colormap="tab10", edgecolor="white")
        axes[1].set_title("Régimen de Seguridad Social por Año (%)", fontweight="bold")
        axes[1].set_ylabel("Porcentaje")
        axes[1].legend(fontsize=8, loc="upper right")
        axes[1].tick_params(axis="x", rotation=0)

    plt.tight_layout()
    plt.savefig(os.path.join(GRAFICOS, "08_parto_seg_social.png"))
    plt.close()
    print("  ✓ 08_parto_seg_social.png")


# ═══════════════════════════════════════════════════════════════════
#  9. TASAS DE MORTALIDAD (FETAL Y NO FETAL)
# ═══════════════════════════════════════════════════════════════════

def tasas_mortalidad(datasets: dict):
    """Calcula y grafica tasas de mortalidad relativas a nacimientos."""
    print("\n" + "="*70)
    print("  9. TASAS DE MORTALIDAD")
    print("="*70)

    if "nacimientos" not in datasets:
        return

    nac_por_ano = datasets["nacimientos"].groupby("ano").size()

    fig, ax = plt.subplots(figsize=(12, 6))

    for nombre, label, color in [
        ("fetales", "Tasa Mortalidad Fetal (×1000 nac.)", "#FF5722"),
        ("no_fetales", "Tasa Mortalidad General (×1000 nac.)", "#4CAF50"),
    ]:
        if nombre in datasets:
            mort_por_ano = datasets[nombre].groupby("ano").size()
            # Alinear años
            years = sorted(set(nac_por_ano.index) & set(mort_por_ano.index))
            tasa = [(mort_por_ano[y] / nac_por_ano[y]) * 1000 for y in years]
            ax.plot(years, tasa, marker="o", linewidth=2.5, markersize=8,
                   label=label, color=color)
            for x, y in zip(years, tasa):
                ax.annotate(f"{y:.1f}", (x, y), textcoords="offset points",
                           xytext=(0, 12), ha="center", fontsize=10)

    ax.set_title("Tasas de Mortalidad por 1000 Nacidos Vivos — Colombia",
                fontweight="bold")
    ax.set_xlabel("Año")
    ax.set_ylabel("Tasa (×1000 nacidos)")
    ax.legend(fontsize=11)
    ax.set_xticks([2020, 2021, 2022, 2023, 2024])
    plt.tight_layout()
    plt.savefig(os.path.join(GRAFICOS, "09_tasas_mortalidad.png"))
    plt.close()
    print("  ✓ 09_tasas_mortalidad.png")


# ═══════════════════════════════════════════════════════════════════
#  10. RESUMEN EJECUTIVO
# ═══════════════════════════════════════════════════════════════════

def resumen_ejecutivo(datasets: dict):
    """Genera un resumen textual del análisis."""
    print("\n" + "="*70)
    print("  10. RESUMEN EJECUTIVO")
    print("="*70)

    resumen = []
    resumen.append("# Resumen Ejecutivo — Estadísticas Vitales Colombia 2020-2024\n")

    for nombre, label in [("nacimientos", "Nacimientos"), ("fetales", "Muertes Fetales"),
                          ("no_fetales", "Muertes No Fetales")]:
        if nombre not in datasets:
            continue
        df = datasets[nombre]
        resumen.append(f"\n## {label}")
        resumen.append(f"- Total registros: {len(df):,}")
        resumen.append(f"- Período: {int(df['ano'].min())} — {int(df['ano'].max())}")
        resumen.append(f"- Columnas: {len(df.columns)}")

        # Por año
        for year in sorted(df["ano"].unique()):
            n = len(df[df["ano"] == year])
            resumen.append(f"  - {int(year)}: {n:,}")

    # Guardar resumen
    resumen_path = os.path.join(GRAFICOS, "resumen_ejecutivo.md")
    with open(resumen_path, "w", encoding="utf-8") as f:
        f.write("\n".join(resumen))
    print(f"  ✓ Resumen guardado en: {resumen_path}")


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  Análisis Exploratorio — Estadísticas Vitales Colombia  ║")
    print("╚══════════════════════════════════════════════════════════╝\n")

    datasets = cargar_datos()

    if not datasets:
        print("\n✗ No se encontraron datasets consolidados.")
        print("  Ejecute primero: python scripts/procesar_todos.py")
        return

    # Ejecutar análisis
    estadisticas_descriptivas(datasets)
    tendencias_temporales(datasets)
    distribucion_sexo(datasets)
    analisis_geografico(datasets)
    correlaciones_nacimientos(datasets)
    analisis_edad_materna(datasets)
    peso_vs_gestacion(datasets)
    tipo_parto_seg_social(datasets)
    tasas_mortalidad(datasets)
    resumen_ejecutivo(datasets)

    print("\n" + "="*70)
    print(f"  ✅ ANÁLISIS COMPLETO — Gráficos en: {GRAFICOS}")
    print("="*70)


if __name__ == "__main__":
    main()
