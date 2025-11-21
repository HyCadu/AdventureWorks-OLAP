# MUDANÇA CRUCIAL: Usando Airflow 2.10.3 para aceitar o pacote da Microsoft
FROM apache/airflow:2.10.3

USER root

# Instalamos as libs de sistema para garantir que o conector SQL funcione
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         freetds-dev \
         libkrb5-dev \
         libssl-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Agora instalamos o pacote. Como o Airflow é novo, não precisa de gambiarras de versão.
RUN pip install apache-airflow-providers-microsoft-mssql