"""
Crypto Analytics Backfill DAG
=============================
Parameterized on-demand pipeline to backfill historical partitions and re-process dbt models.
"""

from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

TRANSFORM_DIR = os.environ.get(
    "DBT_PROJECT_DIR",
    str(Path(__file__).resolve().parent.parent / "transform")
)
PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", TRANSFORM_DIR)
DBT_EXECUTABLE = os.environ.get("DBT_BIN", "dbt")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="backfill_dag",
    default_args=default_args,
    description="Manual/parameterized historical backfill for dbt models",
    schedule_interval=None,  # Triggered manually or via API
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "start_date": Param("2024-01-01", type="string", description="Start date (YYYY-MM-DD)"),
        "end_date": Param("2024-01-02", type="string", description="End date (YYYY-MM-DD)"),
        "models": Param("fct_crypto_trades hourly_crypto_summary", type="string", description="Space-separated dbt models to run"),
        "full_refresh": Param(False, type="boolean", description="Perform full refresh on incremental tables"),
    },
    tags=["backfill", "dbt", "snowflake", "manual"],
)

def run_dbt_backfill(**context):
    params = context["params"]
    start_date = params["start_date"]
    end_date = params["end_date"]
    models = params["models"]
    full_refresh_flag = "--full-refresh" if params["full_refresh"] else ""

    print(f"Triggering backfill for window [{start_date} to {end_date}]")
    print(f"Target models: {models} (full_refresh={params['full_refresh']})")

    # In execution environment, executes dbt with date vars injected into Snowflake SQL models
    cmd = (
        f"cd {TRANSFORM_DIR} && "
        f"{DBT_EXECUTABLE} run --models {models} {full_refresh_flag} "
        f"--vars '{{\"start_date\": \"{start_date}\", \"end_date\": \"{end_date}\"}}' "
        f"--profiles-dir {PROFILES_DIR}"
    )
    print(f"Executing: {cmd}")
    ret = os.system(cmd)
    if ret != 0:
        raise RuntimeError(f"dbt backfill command failed with exit code: {ret}")

with dag:
    start_backfill = EmptyOperator(task_id="start_backfill")

    execute_backfill = PythonOperator(
        task_id="execute_dbt_backfill",
        python_callable=run_dbt_backfill,
        provide_context=True,
    )

    end_backfill = EmptyOperator(task_id="end_backfill")

    start_backfill >> execute_backfill >> end_backfill
