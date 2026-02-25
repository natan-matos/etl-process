import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def extract_csv(file_path: str | Path, encoding: str = "utf-8") -> pd.DataFrame:

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"[BRONZE] File not found: {path}")
    
    logger.info(f"[BRONZE] Reading {path.name}")
    df = pd.read_csv(path, encoding=encoding)

    if df.empty:
        raise ValueError(f"[BRONZE] Empty file: {path}")
    
    logger.info(f" -> {len(df)} rows | columns: {list(df.columns)}")
    return df

def extract_all(bronze_dir: str | Path) -> dict[str, pd.DataFrame]:

    bronze_dir = Path(bronze_dir)

    sources = {
        "vendas": bronze_dir / "vendas" / "vendas.csv",
        "clientes": bronze_dir / "clientes" / "clientes.csv"
    }

    return {name: extract_csv(path) for name, path in sources.items()}
