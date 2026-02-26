"""transform.py"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

KNOWN_OPERADORAS = ["Tim", "Vivo", "Claro", "Oi"]

# ---------------------------------------------------------------------------
# SILVER — typing and normalization
# ---------------------------------------------------------------------------

def silver_clientes(df: pd.DataFrame) -> pd.DataFrame:
    """
        Bronze -> Silver: cast types, normalize strings, extract flags.
    """

    df = df.copy()
    df.dropna(how="all", inplace=True)
    df["id_cliente"] = pd.to_numeric(df["id_cliente"], errors="coerce").astype("Int64")
    df["nome"]       = df["nome"].str.strip().str.title()
    df["email"]      = df["email"].str.strip().str.lower()

    invalid = (~df["email"].str.contains(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", regex=True, na=False)).sum()
    if invalid:
        logger.warning(f"[SILVER] clientes: {invalid} invalid email(s) detected (kept).")

    logger.info(f"[SILVER] clientes: {len(df)} rows")
    return df

def silver_vendas(df: pd.DataFrame) -> pd.DataFrame:
    """
    """
    df = df.copy()
    df.dropna(how="all", inplace=True)
    df["data"]       = pd.to_datetime(df["data"], errors="coerce")
    df["valor"]      = pd.to_numeric(df["valor"], errors="coerce").round(2)
    df["id_cliente"] = pd.to_numeric(df["id_cliente"], errors="coerce").astype("Int64")
    df["produto"]    = df["produto"].str.strip()
    df["is_boleto"]  = df["produto"].str.contains("boleto", case=False, na=False)
    df["produto"]    = df["produto"].str.replace(
        r"\s*-\s*boleto", "", case=False, regex=True
    ).str.strip()

    logger.info(f"[SILVER] vendas: {len(df)} rows")
    return df

def to_silver(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:

    return {
        "clientes": silver_clientes(raw["clientes"]),
        "vendas": silver_vendas(raw["vendas"])
    }

# ---------------------------------------------------------------------------
# GOLD — business rules + star schema
# ---------------------------------------------------------------------------

def gold_dim_cliente(clientes: pd.DataFrame) -> pd.DataFrame:
    """
    Silver -> Gold
    """

    df = clientes.copy()
    df = df.dropna(subset=["id_cliente"])
    df = df.drop_duplicates(subset=["id_cliente"], keep="last")
    df = df[["id_cliente", "nome", "email"]].reset_index(drop=True)
    logger.info(f"[GOLD] dim cliente: {len(df)} rows")

    return df

def gold_dim_produto(vendas: pd.DataFrame) -> pd.DataFrame:
    """
    Silver -> Gold
    """
    df = vendas.dropna(subset=["produto"]).copy()
    df = df[df["valor"] > 0 ]
    dim = (
        df[["produto"]]
        .drop_duplicates(subset=["produto"])
        .rename(columns={"produto": "nome_produto"})
        .assign(
            operadora=lambda d: d["nome_produto"].apply(_get_operadora),
            categoria=lambda d: d["nome_produto"].apply(_get_categoria)
        )
        [["nome_produto", "operadora", "categoria"]]
        .reset_index(drop=True)
    )

    logger.info(f"[GOLD] dim_produto: {len(dim)} rows")
    return dim

def gold_dim_date(vendas: pd.DataFrame) -> pd.DataFrame:
    """
    Silver -> Gold
    """
    df = vendas.dropna(subset=["data"]).copy()
    dim = (
        df[["data"]]
        .drop_duplicates()
        .assign(
            ano        = lambda d: d["data"].dt.year,
            mes        = lambda d: d["data"].dt.month,
            dia        = lambda d: d["data"].dt.day,
            trimestre  = lambda d: d["data"].dt.quarter,
            dia_semana = lambda d: d["data"].dt.dayofweek,
            nome_mes   = lambda d: d["data"].dt.strftime("%B"),
        )
        .sort_values("data")
        .reset_index(drop=True)
    )
    logger.info(f"[GOLD] dim_data: {len(dim)} rows")
    return dim

def gold_fato_vendas(
        vendas: pd.DataFrame,
        dim_cliente: pd.DataFrame,
        dim_produto: pd.DataFrame,
        dim_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Silver -> Gold
    """

    df = vendas.copy()

    # Business rules
    df = df.dropna(subset=["data", "valor", "id_cliente", "produto"])
    df = df[df["valor"] > 0]
    df = df.drop_duplicates()

    df = df.rename(columns={"produto": "nome_produto"})

    # Referential integrity
    before = len(df)
    df = df[
        df["id_cliente"].isin(dim_cliente["id_cliente"]) &
        df["nome_produto"].isin(dim_produto["nome_produto"]) &
        df["data"].isin(dim_data["data"])
    ]
    if before - len(df):
        logger.warning(f"[GOLD] fato_vendas: {before - len(df)} row(s) failed referential integrity — dropped.")

    fato = df[["id_cliente", "nome_produto", "data", "valor", "is_boleto"]].reset_index(drop=True)
    logger.info(f"[GOLD] fato_vendas: {len(fato)} rows")
    return fato

def to_gold(silver: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Orchestrate
    """
    dim_cliente = gold_dim_cliente(silver["clientes"])
    dim_produto = gold_dim_produto(silver["vendas"])
    dim_data = gold_dim_date(silver["vendas"])
    fato_vendas = gold_fato_vendas(silver["vendas"], dim_cliente, dim_produto, dim_data)

    return {
        "dim_cliente": dim_cliente,
        "dim_produto": dim_produto,
        "dim_data": dim_data,
        "fato_vendas": fato_vendas
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_operadora(produto: str) -> str:
    for op in KNOWN_OPERADORAS:
        if op.lower() in produto.lower():
            return op
    return "Desconhecida"

def _get_categoria(produto: str) -> str:
    p = produto.lower()
    if "controle" in p: return "Controle"
    if "net" in p: return "Net"
    return "Outros"