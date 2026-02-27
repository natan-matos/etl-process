# ETL Vendas Diário — Arquitetura Medallion

Pipeline **Bronze → Silver → Gold** orquestrado com Airflow e carga final em PostgreSQL (Docker) modelado em estrela.

---

## Estrutura

```
├── dags/dag.py        # Orquestração Airflow
├── src/
│   ├── extract.py     # Leitura CSV (Bronze)
│   ├── transform.py   # Transformações Silver e Gold
│   └── load.py        # Escrita Parquet e carga no DW
└── storage/
    ├── bronze/        # CSVs particionados por data
    ├── silver/        # Parquet normalizado
    └── gold/          # Parquet star schema
```

---

## Star Schema

```
  dim_cliente       dim_produto       dim_data
      │                  │                │
      └──────────────────┼────────────────┘
                         ▼
                    fato_vendas
             (sk_cliente, sk_produto,
              sk_data, valor, is_boleto)
```



## Pipeline

| Task | O que faz |
|---|---|
| `extract` | Lê CSVs do Bronze, grava Parquet na Silver |
| `silver` | Tipagem, normalização, flag `is_boleto` |
| `gold` | Gera dimensões e fato, valida integridade referencial |
| `load_dw` | Trunca e recarrega o DW; valida contagem pós-carga |

---

## Pontos de Melhoria

- **Testes automatizados** — nenhum `pytest` implementado; as funções de transformação são as principais candidatas
- **XCom com DataFrames** — serializar DataFrames no XCom não escala; `load_dw` deveria ler direto do Parquet em disco
- **Credenciais com fallback hardcoded** — se a variável de ambiente não estiver setada, sobe silenciosamente com senha padrão
- **`mode="incremental"` não implementado** — o parâmetro existe na assinatura de `load_dw` mas não tem lógica de upsert
- **Chave de `dim_produto` frágil** — qualquer variação no `nome_produto` cria duplicata; um hash do nome normalizado seria mais robusto

---

## Como Rodar

**1. Subir o PostgreSQL**
```bash
docker-compose up -d
```

Credenciais padrão (sobrescreva com variáveis de ambiente):

| Variável | Padrão |
|---|---|
| `POSTGRES_USER` | `dwuser` |
| `POSTGRES_PASSWORD` | `dwpassword` |
| `POSTGRES_DB` | `datawarehouse` |
| `POSTGRES_PORT` | `5432` |

**2. Instalar dependências**
```bash
pip install -r requirements.txt
```

**3. Iniciar o Airflow**
```bash
airflow db init
airflow scheduler &
airflow webserver --port 8080
```

**4. Adicionar os CSVs de entrada**
```
storage/bronze/clientes/date=YYYY-MM-DD/clientes.csv
storage/bronze/vendas/date=YYYY-MM-DD/vendas.csv
```

Ative a DAG `etl_vendas_diario` em `http://localhost:8080`.

---