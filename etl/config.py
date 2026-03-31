from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DATA = BASE_DIR / "data" / "raw"
PROCESSED_DATA = BASE_DIR / "data" / "processed"

YEARS = [2020, 2021, 2022, 2023, 2024]

DATASETS = {
    "fetal": "fetal",
    "nofetal": "nofetal",
    "nacimientos": "nac"
}