"""
transformer.py
Medallion Architecture — Bronze to Silver transformation.

Takes raw DataFrames from the Bronze layer and applies:
  - Type casting and date parsing
  - Null / duplicate / invalid value removal
  - String normalization (title case, lowercase)
  - Surrogate key generation
  - Derived columns (year, month, day, valor_sem_impostos)
  - Referential integrity check (vendas <-> clientes)

Output is the Silver layer: clean, typed, enriched DataFrames
ready to be written as Parquet and later promoted to Gold.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bronze -> Silver: per-table cleaning
# ---------------------------------------------------------------------------

def _clean_clientes(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize the clientes table."""
    logger.info("[SILVER] Cleaning clientes ...")
    df = df.copy()

    before = len(df)
    df.drop_duplicates(subset=["id_cliente"], keep="last", inplace=True)
    logger.info(f"  -> Duplicates removed: {before - len(df)}")

    df.dropna(subset=["id_cliente"], inplace=True)

    df["id_cliente"] = df["id_cliente"].astype(int)
    df["nome"]  = df["nome"].str.strip().str.title()
    df["email"] = df["email"].str.strip().str.lower()

    invalid_emails = (~df["email"].str.contains(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", regex=True)).sum()
    if invalid_emails:
        logger.warning(f"  -> {invalid_emails} invalid email(s) detected.")

    logger.info(f"  -> {len(df)} clientes after cleaning.")
    return df


def _clean_vendas(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize the vendas table."""
    logger.info("[SILVER] Cleaning vendas ...")
    df = df.copy()

    df["data"] = pd.to_datetime(df["data"], errors="coerce")

    before = len(df)
    df.dropna(subset=["data", "valor", "id_cliente"], inplace=True)
    logger.info(f"  -> Invalid rows removed: {before - len(df)}")

    df = df[df["valor"] > 0]

    df["id_cliente"] = df["id_cliente"].astype(int)
    df["valor"]   = df["valor"].round(2)
    df["produto"] = df["produto"].str.strip()

    logger.info(f"  -> {len(df)} vendas after cleaning.")
    return df


# ---------------------------------------------------------------------------
# Silver -> Gold: star schema modeling
# ---------------------------------------------------------------------------

def build_dim_clientes(clientes_raw: pd.DataFrame) -> pd.DataFrame:
    """Build the dim_clientes dimension table.

    Schema:
        sk_cliente  INTEGER  PK  (surrogate key)
        id_cliente  INTEGER      (natural key from source)
        nome        TEXT
        email       TEXT
    """
    df = _clean_clientes(clientes_raw)
    dim = df[["id_cliente", "nome", "email"]].reset_index(drop=True)
    dim.insert(0, "sk_cliente", range(1, len(dim) + 1))

    logger.info(f"[GOLD model] dim_clientes: {len(dim)} rows")
    return dim


def build_fato_vendas(
    vendas_raw: pd.DataFrame,
    dim_clientes: pd.DataFrame,
) -> pd.DataFrame:
    """Build the fato_vendas fact table.

    Schema:
        sk_venda            INTEGER  PK
        sk_cliente          INTEGER  FK -> dim_clientes
        id_cliente          INTEGER      (natural key preserved)
        data                DATE
        ano / mes / dia     INTEGER      (derived — simplify GROUP BY)
        produto             TEXT
        valor               NUMERIC
        valor_sem_impostos  NUMERIC      (derived — valor / 1.12)
    """
    df = _clean_vendas(vendas_raw)

    # Resolve surrogate key via lookup on the dimension
    sk_map = dim_clientes.set_index("id_cliente")["sk_cliente"]
    df["sk_cliente"] = df["id_cliente"].map(sk_map)

    orphans = df["sk_cliente"].isna().sum()
    if orphans:
        logger.warning(f"  -> {orphans} sale(s) with no matching client — dropping.")
    df.dropna(subset=["sk_cliente"], inplace=True)
    df["sk_cliente"] = df["sk_cliente"].astype(int)

    # Date-derived columns
    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month
    df["dia"] = df["data"].dt.day

    # Business-derived column
    df["valor_sem_impostos"] = (df["valor"] / 1.12).round(2)

    fato = df[[
        "sk_cliente", "id_cliente",
        "data", "ano", "mes", "dia",
        "produto", "valor", "valor_sem_impostos",
    ]].reset_index(drop=True)

    fato.insert(0, "sk_venda", range(1, len(fato) + 1))

    logger.info(f"[GOLD model] fato_vendas: {len(fato)} rows")
    return fato


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def transform(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Run all Bronze -> Silver -> Gold model transformations.

    Args:
        raw: {'vendas': DataFrame, 'clientes': DataFrame}

    Returns:
        {'dim_clientes': DataFrame, 'fato_vendas': DataFrame}
    """
    dim_clientes = build_dim_clientes(raw["clientes"])
    fato_vendas  = build_fato_vendas(raw["vendas"], dim_clientes)

    return {
        "dim_clientes": dim_clientes,
        "fato_vendas":  fato_vendas,
    }