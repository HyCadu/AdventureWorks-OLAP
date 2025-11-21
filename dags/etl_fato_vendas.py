from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import logging

log = logging.getLogger(__name__)

def extrair_e_carregar_fato_vendas():
    """
    Executa o processo ETL para a Tabela FatoVendas:
    1. Extrai detalhes de vendas e custos das tabelas OLTP.
    2. Calcula as métricas (ValorVenda, Margem, Desconto).
    3. Carrega no Data Warehouse (PostgreSQL) na tabela FatoVendas.
    """
    log.info("Iniciando a extração e cálculo da Tabela FatoVendas...")
    
    # --- 1. EXTRAÇÃO E TRANSFORMAÇÃO (do SQL Server OLTP) ---
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_source')
    
    # SQL para extrair dados de vendas, calcular KPIs e referenciar o ProductID
    # Nota: Usamos o StandardCost do produto como custo base para calcular a Margem
    sql = """
    SELECT
        SOD.ProductID AS id_produto,
        CAST(SOH.OrderDate AS DATE) AS DataVenda,
        
        -- Métricas (KPIs)
        SOD.LineTotal AS ValorVenda, -- Valor total da linha de pedido (já com desconto aplicado)
        SOD.OrderQty AS Quantidade,
        (SOD.UnitPriceDiscount * SOD.OrderQty * SOD.UnitPrice) AS ValorDesconto,
        
        -- Margem = Valor de Venda (LineTotal) - Custo
        (SOD.LineTotal - (SOD.OrderQty * P.StandardCost)) AS Margem
        
    FROM
        Sales.SalesOrderDetail AS SOD
    INNER JOIN
        Sales.SalesOrderHeader AS SOH ON SOD.SalesOrderID = SOH.SalesOrderID
    INNER JOIN
        Production.Product AS P ON SOD.ProductID = P.ProductID
    -- Filtra apenas transações com dados de produto e custo
    WHERE
        P.StandardCost IS NOT NULL;
    """
    
    try:
        df = mssql_hook.get_pandas_df(sql)
        log.info(f"Dados de Fato extraídos e KPIs calculados: {len(df)} linhas.")
    except Exception as e:
        log.error(f"Erro na extração de Fatos do SQL Server: {e}")
        raise
        
    # --- 2. CARGA (Load no PostgreSQL) ---
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()
    
    try:
        # if_exists='append' ou 'replace'. Para Fatos iniciais, 'replace' é comum
        df.to_sql('fatovendas', engine, schema='public', if_exists='replace', index=False)
        log.info(f"Carga da FatoVendas finalizada com sucesso.")
    except Exception as e:
        log.error(f"Erro na carga do PostgreSQL (FatoVendas): {e}")
        raise

# Definição da DAG
with DAG(
    'etl_fato_vendas', 
    start_date=datetime(2023, 1, 1), 
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'dw', 'fatos']
) as dag:

    tarefa_mover_fatos = PythonOperator(
        task_id='mover_fatos_vendas',
        python_callable=extrair_e_carregar_fato_vendas
    )