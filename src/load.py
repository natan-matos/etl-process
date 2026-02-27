"""load.py"""

import os
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Parquet helpers
# ---------------------------------------------------------------------------

def _write_parquet(df: pd.DataFrame, base_dir: Path, table_name: str, ds:str) -> Path:
    # Cria o diretório de saída particionado por data no estilo Hive (extracted_date=YYYY-MM-DD)
    out = base_dir / table_name / f"extracted_date={ds}"
    out.mkdir(parents=True, exist_ok=True)
    path = out / "data.parquet"
    # Grava o DataFrame como Parquet sem índice usando o engine PyArrow
    df.to_parquet(path, index=False, engine='pyarrow')
    logger.info(f" -> {path} ({len(df)} rows)")
    return path 

def write_silver(silver: dict[str, pd.DataFrame], silver_dir: str | Path, ds: str) -> None:
    """
    Escreve camada Silver como parquet particionado
    """

    logger.info(f"[SILVER] Writing Parquet | date={ds}")
    silver_dir = Path(silver_dir)
    # Itera sobre cada tabela do dicionário e persiste como Parquet particionado
    for name, df in silver.items():
        _write_parquet(df, silver_dir, name, ds)

def write_gold(gold: dict[str, pd.DataFrame], gold_dir: str | Path, ds: str) -> None:
    """
    Escreve camada Gold como Parquet particionado
    """
    logger.info(f"[GOLD] Writing Parquet | date={ds}")
    gold_dir = Path(gold_dir)
    # Itera sobre cada tabela do dicionário e persiste como Parquet particionado
    for name, df in gold.items():
        _write_parquet(df, gold_dir, name, ds)

# ---------------------------------------------------------------------------
# PostgreSQL / Redshift
# ---------------------------------------------------------------------------


# DDL de criação das tabelas do DW; executado com CREATE TABLE IF NOT EXISTS (idempotente)
DDL = [
    """
    CREATE TABLE IF NOT EXISTS dim_cliente (
        sk_cliente SERIAL       PRIMARY KEY,
        id_cliente INTEGER      NOT NULL UNIQUE,
        nome       VARCHAR(255) NOT NULL,
        email      VARCHAR(255) NOT NULL
    );
""",
"""
    CREATE TABLE IF NOT EXISTS dim_produto (
        sk_produto     SERIAL       PRIMARY KEY,
        nome_produto   VARCHAR(255) NOT NULL UNIQUE,
        operadora      VARCHAR(100) NOT NULL,
        categoria      VARCHAR(100) NOT NULL
    );
""",
"""
    CREATE TABLE IF NOT EXISTS dim_data (
        sk_data    SERIAL      PRIMARY KEY,
        data       DATE        NOT NULL UNIQUE,
        ano        SMALLINT    NOT NULL,
        mes        SMALLINT    NOT NULL,
        dia        SMALLINT    NOT NULL,
        trimestre  SMALLINT    NOT NULL,
        dia_semana SMALLINT    NOT NULL,
        nome_mes   VARCHAR(20) NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fato_vendas (
        sk_venda           SERIAL         PRIMARY KEY,
        sk_cliente         INTEGER        NOT NULL REFERENCES dim_cliente(sk_cliente),
        sk_produto         INTEGER        NOT NULL REFERENCES dim_produto(sk_produto),
        sk_data            INTEGER        NOT NULL REFERENCES dim_data(sk_data),
        valor              NUMERIC(12, 2) NOT NULL,
        is_boleto          BOOLEAN        NOT NULL DEFAULT FALSE
    );
    """,
]

def get_engine() -> Engine:
    # Constrói a connection string a partir de variáveis de ambiente com valores padrão para desenvolvimento local
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER', 'dwuser')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'dwpassword')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'datawarehouse')}"
    )
    # pool_pre_ping=True verifica a saúde da conexão antes de usá-la (evita erros de conexão expirada)
    return create_engine(url, pool_pre_ping=True)

def load_dw(gold: dict[str, pd.DataFrame], mode: str = "full") -> dict[str, int]:
    """Load Gold tables into PostgreSQL.

    Dimensions are inserted with natural keys — DB generates SKs via SERIAL.
    Fact table is inserted via a staging table so the DB resolves all FKs.

    Args:
        gold: {'dim_cliente': df, 'dim_produto': df, 'dim_data': df, 'fato_vendas': df}
        mode: 'full' (truncate + insert) or 'incremental' (append)

    Returns:
        Row counts per table.
    """

    engine = get_engine()

    # Cria as tabelas no DW caso ainda não existam (idempotente via IF NOT EXISTS)
    with engine.begin() as conn:
        for ddl in DDL:
            conn.execute(text(ddl))

    if mode == "full":
        # No modo full, trunca todas as tabelas e reinicia as sequências SERIAL antes de recarregar
        with engine.begin() as conn:
            conn.execute(text(
                "TRUNCATE TABLE fato_vendas, dim_cliente, dim_produto, dim_data "
                "RESTART IDENTITY CASCADE;"
            ))
        logger.info("[DW] Tables truncated.")
    

    # Carrega as dimensões diretamente; o banco gera os SKs via SERIAL automaticamente
    for table in ("dim_cliente", "dim_produto", "dim_data"):
        df = gold[table].copy()
        # Converte datas para string para evitar erros de serialização no to_sql
        if "data" in df.columns:
            df["data"] = df["data"].astype(str)
        df.to_sql(table, engine, if_exists='append', index=False, method="multi")
        logger.info(f"[DW] {table}: {len(df)} rows loaded")

    # A fato_vendas é carregada via tabela de staging para que o banco resolva os FKs (SKs) por join
    fato = gold["fato_vendas"].copy()
    # Converte a coluna data para string antes de gravar na staging
    fato['data'] = fato["data"].astype(str)
    # Grava (ou substitui) a tabela de staging temporária com os dados da fato
    fato.to_sql("fato_stage", engine, if_exists="replace", index=False, method="multi")

    with engine.begin() as conn:
        # Resolve os SKs fazendo JOIN da staging com cada dimensão pelas chaves de negócio
        conn.execute(text( """
            INSERT INTO fato_vendas (sk_cliente, sk_produto, sk_data, valor, is_boleto)
                SELECT
                    c.sk_cliente,
                    p.sk_produto,
                    d.sk_data,
                    s.valor,
                    s.is_boleto
                FROM fato_stage s
                JOIN dim_cliente   c ON c.id_cliente     = s.id_cliente
                JOIN dim_produto   p ON p.nome_produto   = s.nome_produto
                JOIN dim_data      d ON d.data           = s.data::date;
        """ ))
        # Remove a tabela de staging após a carga para não deixar dados temporários no DW
        conn.execute(text("DROP TABLE IF EXISTS fato_stage;"))
    logger.info(f"[DW] fato_vendas: {len(fato)} rows loaded.")

    # Conta as linhas em cada tabela para validação pós-carga
    counts = {}
    with engine.connect() as conn:
        for table in ("dim_cliente", "dim_produto", "dim_data", "fato_vendas"):
            counts[table] = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
    logger.info(f"[DW] Validation: {counts}")

    return counts