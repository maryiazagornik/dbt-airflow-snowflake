FROM apache/airflow:2.10.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


USER airflow

RUN pip install --no-cache-dir uv

RUN uv pip install --no-cache \
        dbt-snowflake==1.8.3 \
        apache-airflow-providers-snowflake \
        loguru \
        requests


USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow
ENTRYPOINT ["/entrypoint.sh"]
