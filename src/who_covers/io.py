from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]  # project root
DATA = ROOT / "data"
RAW = DATA / "raw"
PROCESSED = DATA / "processed"
for p in (RAW, PROCESSED):
    p.mkdir(parents=True, exist_ok=True)

def raw_path(name: str) -> Path:
    return RAW / name

def processed_path(name: str) -> Path:
    return PROCESSED / name

def save_parquet(df: pd.DataFrame, path: Path):
    df.to_parquet(path, index=False)

def save_csv(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False)
