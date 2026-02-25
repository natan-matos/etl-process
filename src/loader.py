"""
extractor.py
Medallion Architecture — Bronze Layer ingestion.

Reads raw CSV files from storage/bronze/ with zero transformation.
The Bronze layer preserves the source data exactly as received —
no cleaning, no type casting, no business logic.

Expected structure:
    storage/bronze/
        clientes/clientes.csv
        vendas/vendas.csv
"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_csv(filepath: str | Path, encoding: str = "utf-8") -> pd.DataFrame:
    """Read a single CSV file from the Bronze layer into a DataFrame.

    Args:
        filepath: Path to the CSV file.
        encoding: File encoding (default utf-8).

    Returns:
        Raw DataFrame — untouched.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is empty.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"[BRONZE] File not found: {path}")

    logger.info(f"[BRONZE] Reading {path.name} ...")
    df = pd.read_csv(path, encoding=encoding)

    if df.empty:
        raise ValueError(f"[BRONZE] Empty file: {path}")

    logger.info(f"  -> {len(df)} rows | columns: {list(df.columns)}")
    return df


def extract_all(bronze_dir: str | Path) -> dict[str, pd.DataFrame]:
    """Extract all expected source files from the Bronze layer.

    Each source lives in its own subdirectory:
        bronze/clientes/clientes.csv
        bronze/vendas/vendas.csv

    Args:
        bronze_dir: Root of the Bronze layer.

    Returns:
        {'vendas': DataFrame, 'clientes': DataFrame}
    """
    bronze_dir = Path(bronze_dir)

    sources = {
        "vendas":   bronze_dir / "vendas"   / "vendas.csv",
        "clientes": bronze_dir / "clientes" / "clientes.csv",
    }

    return {name: extract_csv(path) for name, path in sources.items()}