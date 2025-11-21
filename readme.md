# 🎯 Projeto Prático: Data Warehouse e ETL com Apache Airflow

Este projeto foi desenvolvido como parte da disciplina de **Engenharia de Dados da UniSales**. O objetivo foi construir um pipeline de dados completo (End-to-End) para migrar dados de um banco transacional (OLTP) para um Data Warehouse (OLAP), utilizando Apache Airflow, Docker e PostgreSQL.

---

## 📋 Escopo do Projeto

O projeto utilizou o dataset público **AdventureWorks2022** (SQL Server) para simular um cenário real de empresa de varejo.

- **Origem:** SQL Server Express (Rodando no Host Windows)
- **Orquestrador:** Apache Airflow (Rodando em Docker/Linux)
- **Destino:** PostgreSQL (Rodando em Docker)
- **Modelagem:** Esquema Estrela (Star Schema)

---

## 🛠️ Tecnologias e Arquitetura

| Categoria | Tecnologia |
|-----------|-----------|
| **Linguagem** | Python 3.12 (Pandas, SQLAlchemy) |
| **Infraestrutura** | Docker & Docker Compose |
| **Banco de Dados Origem** | Microsoft SQL Server 2022 Express |
| **Banco de Dados Destino** | PostgreSQL 13 |
| **Bibliotecas Chave** | apache-airflow-providers-microsoft-mssql, pymssql, pandas |

---

## 🚧 Desafios Técnicos e Soluções (Troubleshooting)

Durante o desenvolvimento, diversos desafios de infraestrutura e conectividade entre o ambiente Docker (Linux) e o Host (Windows) foram superados. Abaixo, o registro técnico das soluções:

### 1. O "Inferno de Dependências" do Driver MS SQL

**Problema:** A instalação do pacote `pymssql` falhava no container Linux devido à falta de compiladores C++ e bibliotecas de sistema, além de conflitos de versão com o Cython 3.0.

**Solução:** Criação de um Dockerfile personalizado estendendo a imagem oficial do Airflow.
- Instalação de dependências de sistema: `build-essential`, `freetds-dev`, `libssl-dev`
- Atualização da imagem base para Airflow 2.10.3 para garantir compatibilidade moderna

### 2. Bloqueio de Rede (Erro 20009)

**Problema:** O Airflow não conseguia alcançar o SQL Server (*Adaptive Server is unavailable*), mesmo usando `host.docker.internal`.

**Causa:** O Firewall do Windows bloqueava conexões de entrada na porta padrão.

**Solução:**
- Criação de Regra de Entrada no Firewall do Windows permitindo tráfego TCP na porta 1433
- Configuração do SQL Server Configuration Manager para escutar explicitamente no protocolo TCP/IP

### 3. Erro de Protocolo/Criptografia (Erro 20017 - EOF)

**Problema:** A conexão era estabelecida (handshake), mas caía imediatamente com *Unexpected EOF*.

**Causa:** O SQL Server forçava criptografia SSL que o driver legado do Linux não conseguia negociar, ou usava portas dinâmicas.

**Solução:**
- Fixação da porta do SQL Server em 1433 (Remoção de portas dinâmicas no IPAll)
- Desativação da opção "Force Encryption" nas propriedades de rede do SQL Server

### 4. Case Sensitivity no PostgreSQL

**Problema:** Erros de `column does not exist` ao tentar consultar dados carregados pelo Pandas.

**Solução:** Padronização de nomes de tabelas em minúsculas (`fatovendas`) e uso de aspas duplas (`"ValorVenda"`) nas queries SQL para respeitar a sensibilidade a maiúsculas/minúsculas do PostgreSQL.

---

## ⚙️ Configuração e Comandos Utilizados

### 1. Preparação do Ambiente (Docker)

O arquivo `docker-compose.yaml` foi configurado para subir os serviços do Airflow e do Postgres.

**Comando para subir a infraestrutura (com build da imagem):**
```bash
docker-compose up -d --build
```

**Comando para acessar o container do Airflow (Manutenção):**
```bash
docker exec -it trabalho_etl-airflow-1 bash
```

### 2. Preparação do Banco de Dados (SQL)

Criação das tabelas no Data Warehouse (PostgreSQL):

**Comando para acessar o Postgres:**
```bash
docker exec -it trabalho_etl-postgres_dw-1 psql -U airflow -d data_warehouse
```

**DDL (Exemplo FatoVendas):**
```sql
CREATE TABLE fatovendas (
    id_produto INT,
    DataVenda DATE,
    "ValorVenda" DECIMAL(19, 4),
    "Quantidade" INT,
    "Margem" DECIMAL(19, 4)
    -- ... outros campos
);
```

---

## 🚀 O Processo ETL (DAGs)

Foram desenvolvidas duas DAGs principais em Python:

### 1. `etl_dimensao_produto.py`

- **Extração:** Busca dados de `Production.Product` e `Production.ProductSubcategory`
- **Transformação:** Join para obter nomes de categorias e filtro de produtos acabados
- **Carga:** Salva na tabela `dim_produto` no Postgres (modo replace)

### 2. `etl_fato_vendas.py`

- **Extração:** Busca dados massivos de `Sales.SalesOrderDetail` e `Sales.SalesOrderHeader`
- **Transformação (Pandas):** Cálculo de métricas de negócio durante o voo:
  - `Margem = LineTotal - (OrderQty * StandardCost)`
  - `ValorDesconto`
- **Carga:** Salva na tabela `fatovendas` (modo replace)

---

## 📊 Resultados e KPIs

O processo foi concluído com sucesso, carregando **295 produtos** e **121.317 registros de vendas**. Os seguintes indicadores foram validados via SQL no Data Warehouse:

✅ Receita Total  
✅ Quantidade Total Vendida  
✅ Ticket Médio  
✅ Margem Bruta Média  
✅ Taxa de Desconto  
✅ Vendas por Categoria  
✅ Margem por Categoria  
✅ Top 5 Produtos (Receita)  
✅ Custo Total vs Receita  
✅ Top Produtos por Desconto  

---

**Desenvolvido por:** Carlos | **Instituição:** UniSales | **Ano:** 2025