import pandas as pd
import logging 
from pathlib import Path 

logger = logging.getLogger(__name__)

def _clean_clients(df: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("[SILVER] Cleaning Clients")
    df = df.copy()

    before = len(df)
    df.drop_duplicates(subset=['id_cliente'], keep="last", inplace=True)
    logger.info(f" -> Duplicates removed: {before - len(df)}")

    df.dropna(subset=["id_cliente"], inplace=True)

    df["id_cliente"] = df["id_cliente"].astype(int)
    df["nome"] = df["nome"].str.strip().str.title()
    df["email"] = df["email"].str.strip().str.lower()

    invalid_emails = (~df["email"].str.contains(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", regex=True)).sum()
    if invalid_emails:
        logger.warning(f" -> {invalid_emails} invalid email(s) detected.")

    
    logger.info(f" -> {len(df)} clients after cleaning")


def _clean_vendas(df: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("[SILVER] Claning vendas")
    df = df.copy()

    df["data"] = pd.to_datetime(df["data"], errors='coerce')

    before = len(df)
    df.dropna(subset=["data", "valor", "id_cliente"], inplace=True)

    df["id_cliente"] = df["id_cliente"].astype(int)
    df["valor"] = df["valor"].astype(float).round(2)
    df["produto"] = df["produto"].str.strip()
    df["is_boleto"] = df["produto"].str.contains("boleto", case=False, na=False)

    logger.info(f" -> {len(df)} vendas after cleaning")

    return df


def build_dim_clients(clients_raw: pd.DataFrame) -> pd.DataFrame:

    df = _clean_clients(clients_raw)
    dim = df[["id_client", "nome", "email"]].reset_index(drop=True)
    dim.insert(0, "sk_client", range(1, len(dim) + 1))

    logger.info(f"[GOLD model] dim_clients: {len(dim)} rows")
    return dim

def build_fact_vendas(
        vendas_raw: pd.DataFrame,
        dim_clients: pd.DataFrame
) -> pd.DataFrame:
    
    df = _clean_vendas(vendas_raw)

    sk_map = dim_clients.set_index("id_cliente")["sk_cliente"]
    df["sk_cliente"] = df["id_cliente"].map(sk_map)

    orphans = df["sk_cliente"].isna().sum()
    if orphans:
        logger.warning(f" -> {orphans} sales(s) with no matching client - dropping")
    
    df.dropna(subset=["sk_cliente"], inplace=True)
    df["sk_cliente"] = df["sk_cliente"].astype(int)

    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month
    df["dia"] = df["data"].dt.day

    fato = df[[
        "sk_cliente", "id_cliente",
        "data", "ano", "mes", "dia",
        "produto", "valor"
    ]].reset_index(drop=True)

    fato.insert(0, "sk_vendas", range(1, len(fato) +1))

    logger.info(f"[GOLD model] fato_vendas: {len(fato)} rows")

    return fato