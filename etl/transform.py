from .cleaning import clean_dataframe
from .normalization import normalize_text, normalize_dates
from .features import create_features

def process_dataframe(df):
    df = clean_dataframe(df)
    df = normalize_text(df)
    df = normalize_dates(df)
    df = create_features(df)
    return df