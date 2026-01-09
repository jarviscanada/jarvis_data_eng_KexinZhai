from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make sure /opt/airflow is on sys.path so "import src.*" works reliably inside the container.
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # /opt/airflow
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import pipeline modules
from src.ingestion.ingest_companies import ingest_companies_to_bronze
from src.ingestion.ingest_prices import ingest_prices_to_bronze
from src.validation.data_quality_checks import bronze_checks, silver_checks, gold_checks
from src.transformations.bronze_to_silver import (
    transform_companies_bronze_to_silver,
    transform_prices_bronze_to_silver,
)
from src.transformations.silver_to_gold import (
    create_gold_enriched,
    create_gold_analytics,
)

logger = logging.getLogger(__name__)

ALERT_EMAILS = os.getenv("ALERT_EMAILS", "")
ALERT_EMAIL_LIST = [e.strip() for e in ALERT_EMAILS.split(",") if e.strip()]


def notify_failure(context: dict) -> None:
    """
    Simple failure callback.
    Note: Email sending requires SMTP config in Airflow; otherwise you still have retries + logs.
    """
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown_task"
    run_id = context.get("run_id", "unknown_run")
    exc = context.get("exception")
    logger.error("[ALERT] DAG=%s TASK=%s RUN=%s ERROR=%s", dag_id, task_id, run_id, exc)


def log_success(**context) -> None:
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    run_id = context.get("run_id", "unknown_run")
    logger.info("[SUCCESS] DAG=%s RUN=%s finished successfully.", dag_id, run_id)


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email": ALERT_EMAIL_LIST,
    "email_on_failure": True if ALERT_EMAIL_LIST else False,
    "email_on_retry": False,
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="equity_market_etl",
    default_args=default_args,
    description="Batch ETL for equity market data (bronze/silver/gold) with quality checks",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["market-data", "batch", "medallion"],
) as dag:
    t_ingest_companies = PythonOperator(
        task_id="ingest_companies_to_bronze",
        python_callable=ingest_companies_to_bronze,
    )

    t_ingest_prices = PythonOperator(
        task_id="ingest_prices_to_bronze",
        python_callable=ingest_prices_to_bronze,
    )

    t_bronze_checks = PythonOperator(
        task_id="bronze_data_quality_checks",
        python_callable=bronze_checks,
    )

    t_companies_silver = PythonOperator(
        task_id="transform_companies_bronze_to_silver",
        python_callable=transform_companies_bronze_to_silver,
    )

    t_prices_silver = PythonOperator(
        task_id="transform_prices_bronze_to_silver",
        python_callable=transform_prices_bronze_to_silver,
    )

    t_silver_checks = PythonOperator(
        task_id="silver_data_quality_checks",
        python_callable=silver_checks,
    )

    t_gold_enriched = PythonOperator(
        task_id="create_gold_prices_enriched",
        python_callable=create_gold_enriched,
    )

    t_gold_analytics = PythonOperator(
        task_id="create_gold_analytics",
        python_callable=create_gold_analytics,
    )

    t_gold_checks = PythonOperator(
        task_id="gold_data_quality_checks",
        python_callable=gold_checks,
    )

    t_success = PythonOperator(
        task_id="pipeline_success",
        python_callable=log_success,
    )

    # Dependencies
    # 1) Ingest both datasets to bronze
    [t_ingest_companies, t_ingest_prices] >> t_bronze_checks
    # 2) Bronze -> Silver transforms
    t_bronze_checks >> [t_companies_silver, t_prices_silver] >> t_silver_checks
    # 3) Silver -> Gold + Analytics
    t_silver_checks >> t_gold_enriched >> t_gold_analytics >> t_gold_checks >> t_success
