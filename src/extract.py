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

    # Garante que bronze_dir seja um objeto Path, independente de vir como string ou Path
    bronze_dir = Path(bronze_dir)

    # Mapeia os nomes das tabelas para seus caminhos particionados por data (Hive-style: date=YYYY-MM-DD)
    sources = {
        "clientes": bronze_dir / "clientes" / f"date={ds}" / "clientes.csv",
        "vendas": bronze_dir / "vendas" / f"date={ds}" / "vendas.csv"
    }

    # Dicionário que acumulará os DataFrames lidos com sucesso
    result = {}

    for name, path in sources.items():
        # Verifica se a partição existe antes de tentar ler; falha rápido com mensagem clara
        if not path.exists():
            raise FileNotFoundError(
                f"[BRONZE] Particao nao encontrada para {name} em {ds}."
            )
        
        # Lê o CSV da partição correspondente
        df = pd.read_csv(path)

        # Rejeita arquivos vazios para evitar propagação silenciosa de dados incompletos
        if df.empty:
            raise ValueError(f"[BRONZE] Particao vazia: {path}")
        
        logger.info(f"[BRONZE] {name} | date={ds} | {len(df)} rows")
        result[name] = df

    return result