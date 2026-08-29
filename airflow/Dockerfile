FROM apache/airflow:2.9.3-python3.11
USER root

RUN apt-get update && apt-get install -y --no-install-recommends docker.io docker-compose-plugin \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt


USER airflow
RUN export AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    && python -m pip install --no-cache-dir psycopg2-binary -c "$AIRFLOW_CONSTRAINTS_URL" \
    && pip install --no-cache-dir -r /requirements.txt -c "$AIRFLOW_CONSTRAINTS_URL"