FROM apache/airflow:2.9.2-python3.11

USER root

# Install system dependencies (build-essential, git, curl)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        git \
        curl \
        libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ENV PATH="/home/airflow/.local/bin:${PATH}"

# Install Python packages using modified Airflow constraints (removing conflicting keyring)
ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.11

RUN curl -sSL "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    | grep -v '^keyring==' > /tmp/constraints.txt \
    && pip install --no-cache-dir \
        --constraint /tmp/constraints.txt \
        "apache-airflow==${AIRFLOW_VERSION}" \
        "dbt-core>=1.8.0,<1.9.0" \
        "dbt-snowflake>=1.8.0,<1.9.0" \
        "astronomer-cosmos>=1.4.0,<1.5.0" \
        "confluent-kafka>=2.3.0" \
        "duckdb>=0.10.0" \
        "pydantic>=2.5.3" \
        "websocket-client>=1.7.0" \
    && rm /tmp/constraints.txt

# Set dbt and Airflow environment variables
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
ENV DBT_PROJECT_DIR=/opt/airflow/transform
ENV DBT_PROFILES_DIR=/opt/airflow/transform
ENV PYTHONPATH=/opt/airflow
