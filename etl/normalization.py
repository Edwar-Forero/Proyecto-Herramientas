import pandas as pd

def normalize_text(df: pd.DataFrame) -> pd.DataFrame:
    
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.upper().str.strip()

    return df


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    
    for col in df.columns:
        if "fecha" in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')

    return df