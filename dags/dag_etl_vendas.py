"""dag"""        
from __future__ import annotations

import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path



from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Resolve o diretório src (código-fonte) e storage (dados) a partir da localização desta DAG
SCR_DIR = Path(__file__).parent.parent / "src"
STORAGE_DIR = Path(__file__).parent.parent / "storage"

# Adiciona o diretório src ao path para permitir imports dos módulos locais (extract, transform, load)
sys.path.insert(0, str(SCR_DIR))

logger = logging.getLogger(__name__)

# Argumentos padrão aplicados a todas as tasks da DAG
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,           # Tenta novamente até 2 vezes em caso de falha
    "retry_delay": timedelta(seconds=1)
}

def task_extract(**context):
    # Imports locais evitam carregar módulos pesados no momento do parse da DAG pelo scheduler
    from extract import extract_all
    from load import _write_parquet   # importe a função helper que já existe no load.py

    ds = context['ds']
    bronze_dir = STORAGE_DIR / "bronze"
    silver_dir = STORAGE_DIR / "silver"   # vamos escrever direto na silver temporariamente

    # Lê todos os CSVs da camada Bronze para a data de execução
    raw = extract_all(bronze_dir, ds=ds)

    # Validação mínima: garante que ambas as tabelas essenciais foram extraídas
    if "vendas" not in raw or "clientes" not in raw:
        raise ValueError(f"[EXTRACT] Dados incompletos: {list(raw.keys())}")

    # Escreve direto como parquet na silver (particionado por ds)
    _write_parquet(raw["clientes"], silver_dir, "clientes", ds)
    _write_parquet(raw["vendas"], silver_dir, "vendas", ds)

    logger.info(f"[BRONZE → SILVER temp] ds={ds} | vendas={len(raw['vendas'])} | clientes={len(raw['clientes'])}")

def task_silver(**context):
    from transform import to_silver
    from load import write_silver

    ds = context["ds"]
    silver_dir = STORAGE_DIR / "silver"

    # Monta os caminhos esperados dos parquets brutos gravados pela task de extração
    clientes_path = silver_dir / "clientes" / f"extracted_date={ds}" / "data.parquet"
    vendas_path   = silver_dir / "vendas"   / f"extracted_date={ds}" / "data.parquet"

    # Falha explicitamente se os arquivos de entrada não existirem, evitando DataFrames vazios silenciosos
    if not clientes_path.exists() or not vendas_path.exists():
        raise FileNotFoundError(f"[SILVER input] Parquet não encontrado: {clientes_path} ou {vendas_path}")

    # Carrega os parquets brutos em memória para processamento
    raw = {
        "clientes": pd.read_parquet(clientes_path),
        "vendas":   pd.read_parquet(vendas_path),
    }

    # Aplica as transformações de tipagem e normalização da camada Silver
    silver = to_silver(raw)
    write_silver(silver, silver_dir, ds=ds)  # isso já existe e escreve as transformadas

    logger.info(f"[SILVER] Processado ds={ds} | silver_clientes={len(silver['clientes'])} | silver_vendas={len(silver['vendas'])}")

def task_gold(**context):
    from transform import to_gold
    from load import write_gold

    ds = context["ds"]
    silver_dir = STORAGE_DIR / "silver"
    
    # Read from Parquet files directly
    clientes_path = silver_dir / "clientes" / f"extracted_date={ds}" / "data.parquet"
    vendas_path = silver_dir / "vendas" / f"extracted_date={ds}" / "data.parquet"
    
    # Carrega as tabelas Silver processadas para gerar as dimensões e fato do modelo estrela
    silver = {
        "clientes": pd.read_parquet(clientes_path),
        "vendas": pd.read_parquet(vendas_path)
    }
    
    # Gera as tabelas Gold (dim_cliente, dim_produto, dim_data, fato_vendas)
    gold = to_gold(silver)
    write_gold(gold, STORAGE_DIR / "gold", ds=ds)
    
    # Push to XCom for the next task if needed
    # Serializa cada tabela Gold como JSON e empurra ao XCom para consumo pela task de carga no DW
    ti = context["ti"]
    for name, df in gold.items():
        ti.xcom_push(key=f"gold_{name}_json", value=df.to_json())
    
    logger.info(f"[GOLD] Processado ds={ds}")

def task_load_dw(**context):
    from load import load_dw
    from io import StringIO

    # Recupera os DataFrames Gold serializados em JSON via XCom da task anterior
    ti = context["ti"]
    gold = {
        # Reconstrói dim_cliente a partir do JSON armazenado no XCom
        "dim_cliente": pd.read_json(StringIO(ti.xcom_pull(task_ids="gold", key="gold_dim_cliente_json"))),
        # Reconstrói dim_produto a partir do JSON armazenado no XCom
        "dim_produto": pd.read_json(StringIO(ti.xcom_pull(task_ids="gold", key="gold_dim_produto_json"))),
        # Reconstrói dim_data garantindo parse correto da coluna de datas
        "dim_data":    pd.read_json(StringIO(ti.xcom_pull(task_ids="gold", key="gold_dim_data_json")),    convert_dates=["data"]),
        # Reconstrói fato_vendas garantindo parse correto da coluna de datas
        "fato_vendas": pd.read_json(StringIO(ti.xcom_pull(task_ids="gold", key="gold_fato_vendas_json")), convert_dates=["data"]),
    }
    # Carrega as tabelas no Data Warehouse e retorna a contagem de linhas por tabela
    counts = load_dw(gold, mode="full")

    # Validação pós-carga: garante que nenhuma tabela ficou vazia no DW
    for table, count in counts.items():
        if count == 0:
            raise ValueError(f"[VALIDATE] '{table}' is empty after load!")
    logger.info(f"[VALIDATE] OK -> {counts}")

        
with DAG(
    dag_id="etl_vendas_diario",
    description="Medallion ETL: Bronze -> Silver -> Gold (PostgresSQL)",
    schedule="0 6 * * *",      # Executa todos os dias às 06:00 UTC
    start_date=datetime(2025,2,1),
    catchup=False,              # Não executa execuções retroativas ao ativar a DAG
    default_args=default_args,
    tags=["etl", "medallion", "vendas"]
) as dag:
    
    # Definição das tasks com seus respectivos callables Python
    extract = PythonOperator(task_id="extract", python_callable=task_extract)
    silver = PythonOperator(task_id="silver", python_callable=task_silver)
    gold = PythonOperator(task_id="gold", python_callable=task_gold)
    load = PythonOperator(task_id="load_dw", python_callable=task_load_dw)
    
    # Dependências lineares: Bronze → Silver → Gold → DW
    extract >> silver >> gold >> load