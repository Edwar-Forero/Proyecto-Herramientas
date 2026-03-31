import pandas as pd

def create_features(df: pd.DataFrame) -> pd.DataFrame:
    
    # ejemplo: edad en rangos
    if "edad_madre" in df.columns:
        df["edad_madre_rango"] = pd.cut(
            df["edad_madre"],
            bins=[0, 18, 30, 45, 100],
            labels=["ADOLESCENTE", "JOVEN", "ADULTA", "MAYOR"]
        )

    # ejemplo: año desde fecha
    for col in df.columns:
        if "fecha" in col.lower():
            df["anio"] = df[col].dt.year

    return df