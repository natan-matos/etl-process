"""transform.py"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Lista de operadoras conhecidas usada para classificação dos produtos
KNOWN_OPERADORAS = ["Tim", "Vivo", "Claro", "Oi"]

# ---------------------------------------------------------------------------
# SILVER — typing and normalization
# ---------------------------------------------------------------------------

def silver_clientes(df: pd.DataFrame) -> pd.DataFrame:
    """
        Bronze -> Silver: cast types, normalize strings, extract flags.
    """

    # Trabalha em uma cópia para não modificar o DataFrame original
    df = df.copy()
    # Remove linhas completamente vazias que possam ter vindo do CSV bruto
    df.dropna(how="all", inplace=True)
    # Converte id_cliente para inteiro nullable (Int64 suporta NaN diferente de int64)
    df["id_cliente"] = pd.to_numeric(df["id_cliente"], errors="coerce").astype("Int64")
    # Normaliza o nome: remove espaços extras e aplica Title Case
    df["nome"]       = df["nome"].str.strip().str.title()
    # Normaliza o email: remove espaços extras e converte para minúsculas
    df["email"]      = df["email"].str.strip().str.lower()

    # Conta e loga e-mails com formato inválido (mantidos no dataset para decisão posterior)
    invalid = (~df["email"].str.contains(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", regex=True, na=False)).sum()
    if invalid:
        logger.warning(f"[SILVER] clientes: {invalid} invalid email(s) detected (kept).")

    logger.info(f"[SILVER] clientes: {len(df)} rows")
    return df

def silver_vendas(df: pd.DataFrame) -> pd.DataFrame:
    """
    """
    # Trabalha em uma cópia para não modificar o DataFrame original
    df = df.copy()
    # Remove linhas completamente vazias
    df.dropna(how="all", inplace=True)
    # Converte a coluna data para datetime; valores inválidos viram NaT
    df["data"]       = pd.to_datetime(df["data"], errors="coerce")
    # Converte valor para float e arredonda para 2 casas decimais (centavos)
    df["valor"]      = pd.to_numeric(df["valor"], errors="coerce").round(2)
    # Converte id_cliente para inteiro nullable
    df["id_cliente"] = pd.to_numeric(df["id_cliente"], errors="coerce").astype("Int64")
    # Remove espaços extras do nome do produto
    df["produto"]    = df["produto"].str.strip()
    # Cria flag booleana indicando se a venda foi realizada via boleto (baseado no nome do produto)
    df["is_boleto"]  = df["produto"].str.contains("boleto", case=False, na=False)
    # Remove o sufixo "- boleto" do nome do produto para padronizar a chave de negócio
    df["produto"]    = df["produto"].str.replace(
        r"\s*-\s*boleto", "", case=False, regex=True
    ).str.strip()

    logger.info(f"[SILVER] vendas: {len(df)} rows")
    return df

def to_silver(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    # Orquestra a transformação Bronze → Silver para cada tabela do dicionário de entrada
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
    # Remove clientes sem id (não podem compor a dimensão sem chave de negócio)
    df = df.dropna(subset=["id_cliente"])
    # Garante unicidade por id_cliente, mantendo o registro mais recente em caso de duplicata
    df = df.drop_duplicates(subset=["id_cliente"], keep="last")
    # Seleciona apenas as colunas necessárias para a dimensão cliente
    df = df[["id_cliente", "nome", "email"]].reset_index(drop=True)
    logger.info(f"[GOLD] dim cliente: {len(df)} rows")

    return df

def gold_dim_produto(vendas: pd.DataFrame) -> pd.DataFrame:
    """
    Silver -> Gold
    """
    # Remove vendas sem produto identificado
    df = vendas.dropna(subset=["produto"]).copy()
    # Exclui vendas com valor zero ou negativo (não representam produtos reais)
    df = df[df["valor"] > 0 ]
    dim = (
        df[["produto"]]
        .drop_duplicates(subset=["produto"])          # Uma linha por produto único
        .rename(columns={"produto": "nome_produto"})
        .assign(
            # Deriva a operadora a partir do nome do produto usando lista de operadoras conhecidas
            operadora=lambda d: d["nome_produto"].apply(_get_operadora),
            # Deriva a categoria a partir de palavras-chave no nome do produto
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
    # Remove vendas sem data válida
    df = vendas.dropna(subset=["data"]).copy()
    dim = (
        df[["data"]]
        .drop_duplicates()          # Uma linha por data única (calendário de datas das vendas)
        .assign(
            ano        = lambda d: d["data"].dt.year,
            mes        = lambda d: d["data"].dt.month,
            dia        = lambda d: d["data"].dt.day,
            trimestre  = lambda d: d["data"].dt.quarter,
            dia_semana = lambda d: d["data"].dt.dayofweek,    # 0=Segunda, 6=Domingo
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
    # Remove registros com campos obrigatórios ausentes (sem eles não é possível montar a fato)
    df = df.dropna(subset=["data", "valor", "id_cliente", "produto"])
    # Descarta vendas com valor inválido (zero ou negativo)
    df = df[df["valor"] > 0]
    # Remove linhas completamente duplicadas
    df = df.drop_duplicates()

    # Renomeia para alinhar com a chave de negócio da dim_produto
    df = df.rename(columns={"produto": "nome_produto"})

    # Referential integrity
    before = len(df)
    # Filtra apenas as vendas cujos FK existem em todas as dimensões (integridade referencial)
    df = df[
        df["id_cliente"].isin(dim_cliente["id_cliente"]) &
        df["nome_produto"].isin(dim_produto["nome_produto"]) &
        df["data"].isin(dim_data["data"])
    ]
    # Loga o número de registros descartados por violação de integridade referencial
    if before - len(df):
        logger.warning(f"[GOLD] fato_vendas: {before - len(df)} row(s) failed referential integrity — dropped.")

    # Seleciona apenas as colunas que comporão a tabela fato no DW
    fato = df[["id_cliente", "nome_produto", "data", "valor", "is_boleto"]].reset_index(drop=True)
    logger.info(f"[GOLD] fato_vendas: {len(fato)} rows")
    return fato

def to_gold(silver: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Orchestrate
    """
    # Gera cada tabela do modelo estrela a partir das tabelas Silver
    dim_cliente = gold_dim_cliente(silver["clientes"])
    dim_produto = gold_dim_produto(silver["vendas"])
    dim_data = gold_dim_date(silver["vendas"])
    # A fato recebe as dimensões para validação de integridade referencial antes da carga
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
    # Percorre a lista de operadoras conhecidas e retorna a primeira que aparecer no nome do produto
    for op in KNOWN_OPERADORAS:
        if op.lower() in produto.lower():
            return op
    # Retorna "Desconhecida" quando nenhuma operadora conhecida é identificada no produto
    return "Desconhecida"

def _get_categoria(produto: str) -> str:
    p = produto.lower()
    # Classifica por palavras-chave no nome do produto; ordem importa (mais específico primeiro)
    if "controle" in p: return "Controle"
    if "net" in p: return "Net"
    return "Outros"