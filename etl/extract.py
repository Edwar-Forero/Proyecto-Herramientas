import pandas as pd
from .config import RAW_DATA

def load_csv(file_name: str) -> pd.DataFrame:
    path = RAW_DATA / file_name
    return pd.read_csv(path, low_memory=False, sep=',', encoding='latin-1')