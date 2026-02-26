"""dag"""        
from __future__ import annotations

import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path



from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

SCR_DIR = Path(__file__).parent.parent / "src"
STORAGE_DIR = Path(__file__).parent.parent / "storage"

sys.path.insert(0, str(SCR_DIR))

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2, 
    "retry_delay": timedelta(seconds=1)
}

def task_extract(**context):
    from extract import extract_all
    from load import _write_parquet   # importe a função helper que já existe no load.py

    ds = context['ds']
    bronze_dir = STORAGE_DIR / "bronze"
    silver_dir = STORAGE_DIR / "silver"   # vamos escrever direto na silver temporariamente

    raw = extract_all(bronze_dir, ds=ds)

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

    clientes_path = silver_dir / "clientes" / f"extracted_date={ds}" / "data.parquet"
    vendas_path   = silver_dir / "vendas"   / f"extracted_date={ds}" / "data.parquet"

    if not clientes_path.exists() or not vendas_path.exists():
        raise FileNotFoundError(f"[SILVER input] Parquet não encontrado: {clientes_path} ou {vendas_path}")

    raw = {
        "clientes": pd.read_parquet(clientes_path),
        "vendas":   pd.read_parquet(vendas_path),
    }

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
    
    silver = {
        "clientes": pd.read_parquet(clientes_path),
        "vendas": pd.read_parquet(vendas_path)
    }
    
    gold = to_gold(silver)
    write_gold(gold, STORAGE_DIR / "gold", ds=ds)
    
    # Push to XCom for the next task if needed
    ti = context["ti"]
    for name, df in gold.items():
        ti.xcom_push(key=f"gold_{name}_json", value=df.to_json())
    
    logger.info(f"[GOLD] Processado ds={ds}")

def task_load_dw(**context):
    from load import load_dw
    from io import StringIO

    ti = context["ti"]
    gold = {
        "dim_cliente": pd.read_json(StringIO(ti.xcom_pull(task_ids="gold", key="gold_dim_cliente_json"))),
        "dim_produto": pd.read_json(StringIO(ti.xcom_pull(task_ids="gold", key="gold_dim_produto_json"))),
        "dim_data":    pd.read_json(StringIO(ti.xcom_pull(task_ids="gold", key="gold_dim_data_json")),    convert_dates=["data"]),
        "fato_vendas": pd.read_json(StringIO(ti.xcom_pull(task_ids="gold", key="gold_fato_vendas_json")), convert_dates=["data"]),
    }
    counts = load_dw(gold, mode="full")
    for table, count in counts.items():
        if count == 0:
            raise ValueError(f"[VALIDATE] '{table}' is empty after load!")
    logger.info(f"[VALIDATE] OK -> {counts}")

        
with DAG(
    dag_id="etl_vendas_diario",
    description="Medallion ETL: Bronze -> Silver -> Gold (PostgresSQL)",
    schedule="0 6 * * *",
    start_date=datetime(2025,2,1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "medallion", "vendas"]
) as dag:
    
    extract = PythonOperator(task_id="extract", python_callable=task_extract)
    silver = PythonOperator(task_id="silver", python_callable=task_silver)
    gold = PythonOperator(task_id="gold", python_callable=task_gold)
    load = PythonOperator(task_id="load_dw", python_callable=task_load_dw)
    
    extract >> silver >> gold >> load