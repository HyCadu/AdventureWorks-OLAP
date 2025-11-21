from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import logging

# Define o logger para a tarefa
log = logging.getLogger(__name__)

def extrair_e_carregar():
    """
    Executa o processo ETL para a Dimensão Produto:
    1. Extrai dados das tabelas OLTP (Production.Product).
    2. Transforma (joins e renomeia colunas).
    3. Carrega no Data Warehouse (PostgreSQL) na tabela dim_produto.
    """
    log.info("Iniciando a extração da Dimensão Produto...")
    
    # --- 1. EXTRAÇÃO (do SQL Server OLTP) ---
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_source')
    
    # Query final corrigida para o modelo Transacional (OLTP)
    # Seleciona o ID, nome, cor e categoria (via JOIN)
    sql = """
    SELECT
        p.ProductID AS id_produto,
        p.Name AS nome,
        p.Color AS cor,
        psc.Name AS categoria
    FROM Production.Product AS p
    LEFT JOIN Production.ProductSubcategory AS psc
        ON p.ProductSubcategoryID = psc.ProductSubcategoryID
    WHERE
        p.FinishedGoodsFlag = 1; -- Filtra apenas produtos acabados
    """
    
    try:
        df = mssql_hook.get_pandas_df(sql)
        log.info(f"Dados extraídos com sucesso: {len(df)} linhas de produtos.")
    except Exception as e:
        log.error(f"Erro na extração do SQL Server: {e}")
        raise
        
    # --- 2. TRANSFORMAÇÃO (Implícita no Pandas/SQL) ---
    # O Pandas já tem o DataFrame pronto para o formato do DW
    
    # --- 3. CARGA (Load no PostgreSQL) ---
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()
    
    try:
        # if_exists='replace' apaga o conteúdo e recria a tabela/estrutura a cada execução
        df.to_sql('dim_produto', engine, schema='public', if_exists='replace', index=False)
        log.info(f"Carga finalizada com sucesso na dim_produto.")
    except Exception as e:
        log.error(f"Erro na carga do PostgreSQL: {e}")
        raise

# Definição da DAG
with DAG(
    'etl_dimensao_produto', 
    start_date=datetime(2023, 1, 1), 
    schedule_interval=None,         # Execução manual
    catchup=False,                  # Ignora execuções retroativas
    tags=['etl', 'dw', 'produto']
) as dag:

    tarefa_mover_dados = PythonOperator(
        task_id='mover_dados',
        python_callable=extrair_e_carregar
    )