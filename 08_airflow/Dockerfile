FROM apache/airflow:2.10.1

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-athena-community

    COPY --chown=airflow:root ../04_dbt_transform /opt/airflow/dbt_transform
RUN cd /opt/airflow/dbt_transform && dbt deps