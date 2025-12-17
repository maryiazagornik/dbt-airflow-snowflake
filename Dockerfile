FROM apache/airflow:2.10.3

USER root

RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install uv

RUN uv pip install --no-cache \
    dbt-snowflake==1.8.3 \
    astronomer-cosmos==1.7.0 \
    apache-airflow-providers-snowflake \
    loguru \
    requests
