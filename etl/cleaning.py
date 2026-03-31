import pandas as pd

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    
    # eliminar duplicados
    df = df.drop_duplicates()

    # eliminar columnas vacías
    df = df.dropna(axis=1, how='all')

    # manejar nulos básicos
    df = df.fillna({
        col: 0 if df[col].dtype in ['int64', 'float64'] else "DESCONOCIDO"
        for col in df.columns
    })

    return df