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
    "retry_delay": timedelta(minutes=5)
}

def task_extract(**context):
    from extract import extract_all

    """Le Particao Bronze para pegar data de execucao"""
    ds = context['ds']
    raw = extract_all(STORAGE_DIR / "bronze", ds=ds)
    context['ti'].xcom_push(key="vendas_json", value=raw["vendas"].to_json())
    context['ti'].xcom_push(key="clientes_json", value=raw["clientes"].to_json())
    logger.info(f"[BRONZE] ds={ds} | vendas={len(raw['vendas'])} | clientes={len(raw['clientes'])}")

def task_silver(**context):
    from transform import to_silver
    from load import write_silver

    ti  = context["ti"]
    ds  = context["ds"]
    raw = {
        "vendas":   pd.read_json(ti.xcom_pull(key="vendas_json")),
        "clientes": pd.read_json(ti.xcom_pull(key="clientes_json")),
    }
    silver = to_silver(raw)
    write_silver(silver, STORAGE_DIR / "silver", ds=ds)
    ti.xcom_push(key="silver_vendas_json",   value=silver["vendas"].to_json())
    ti.xcom_push(key="silver_clientes_json", value=silver["clientes"].to_json())

def task_gold(**context):
    from transform import to_gold
    from load import write_gold

    ti     = context["ti"]
    ds     = context["ds"]
    silver = {
        "vendas":   pd.read_json(ti.xcom_pull(key="silver_vendas_json"), convert_dates=["data"]),
        "clientes": pd.read_json(ti.xcom_pull(key="silver_clientes_json")),
    }
    gold = to_gold(silver)
    write_gold(gold, STORAGE_DIR / "gold", ds=ds)
    for name, df in gold.items():
        ti.xcom_push(key=f"gold_{name}_json", value=df.to_json())

def task_load_dw(**context):
    from load import load_dw

    ti   = context["ti"]
    gold = {
        "dim_cliente": pd.read_json(ti.xcom_pull(key="gold_dim_cliente_json")),
        "dim_produto": pd.read_json(ti.xcom_pull(key="gold_dim_produto_json")),
        "dim_data":    pd.read_json(ti.xcom_pull(key="gold_dim_data_json"),    convert_dates=["data"]),
        "fato_vendas": pd.read_json(ti.xcom_pull(key="gold_fato_vendas_json"), convert_dates=["data"]),
    }
    counts = load_dw(gold, mode="full")
    for table, count in counts.items():
        if count == 0:
            raise ValueError(f"[VALIDATE] '{table}' is empty after load!")
    logger.info(f"[VALIDATE] OK -> {counts}")


with DAG(
    dag_id="etl_vendas_diario",
    description="Medallion ETL: Bronze -> Silver -> Gold (PostgresSQL)",
    schedule_invterval="0 6 * * *",
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
