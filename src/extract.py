"""extract.py"""

import pandas as pd
import logging
from pathlib import Path 

logger = logging.getLogger(__name__)

def extract_all(bronze_dir: str | Path, ds: str) -> dict[str, pd.DataFrame]:
    """ Le vendas e clientes para uma data especifica

    Args:
        bronze_dir: Raiz da camada Bronze (caminho local ou S3)
        ds: Data de execucao no formato YYYY-MM-DD vindo do Airflow

    Returns:
        {'vendas': DataFrame, 'clientes': DataFrame}
    """

    bronze_dir = Path(bronze_dir)
    sources = {
        "clientes": bronze_dir / "clientes" / f"date={ds}" / "clientes.csv",
        "vendas": bronze_dir / "vendas" / f"date={ds}" / "vendas.csv"
    }

    result = {}

    for name, path in sources.items():
        if not path.exists():
            raise FileNotFoundError(
                f"[BRONZE] Particao nao encontrada para {name} em {ds}."
            )
        
        df = pd.read_csv(path)

        if df.empty:
            raise ValueError(f"[BRONZE] Particao vazia: {path}")
        
        logger.info(f"[BRONZE] {name} | date={ds} | {len(df)} rows")
        result[name] = df

    return result