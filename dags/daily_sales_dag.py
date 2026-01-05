"""
Daily Sales DAG
----------------
This DAG performs a daily ETL workflow with three main tasks:

1. Extract and Transform - Runs the Spark job to process sales data.
2. Validate - Validates the transformed output files for nulls and existence.
3. Load - Simulates loading the data into the target system (e.g., Snowflake).

Failure alerts are logged if any task fails.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
import os


def task_failure_alert(context):
    """
    Callback function to alert/log when a task fails.

    Args:
        context (dict): Airflow context dictionary containing task and execution details.
    """
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context.get('logical_date')

    # Mock notification: log the failure
    logging.error(f"ALERT! Task {task_id} in DAG {dag_id} failed on {execution_date}.")


# -----------------------------
# Default DAG arguments
# -----------------------------
default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": task_failure_alert
}


# -----------------------------
# Validation Task
# -----------------------------
def validate_output():
    """
    Validate the transformed output files.

    Checks:
        - Output directories exist
        - No null values in category_summary and customer_summary Parquet files

    Raises:
        FileNotFoundError: If output directories are missing
        ValueError: If any null values are found in the outputs
    """
    logging.info("Starting validation of transformed output...")

    # Absolute paths inside the container
    category_path = "/opt/airflow/spark_job/output/category_summary/"
    customer_path = "/opt/airflow/spark_job/output/customer_summary/"

    # Ensure output directories exist
    if not os.path.exists(category_path) or not os.path.exists(customer_path):
        raise FileNotFoundError("Output directories not found!")

    # Read Parquet files
    df_category = pd.read_parquet(category_path)
    df_customer = pd.read_parquet(customer_path)

    # Check for nulls
    if df_category.isnull().any().any():
        raise ValueError("Null values found in category_summary output")

    if df_customer.isnull().any().any():
        raise ValueError("Null values found in customer_summary output")

    logging.info("Output validation passed successfully.")


# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id="daily_sales_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 5),
    schedule_interval="@daily",
    catchup=False,
    doc_md=__doc__,  # DAG-level documentation
) as dag:

    # -----------------------------
    # Transform Task: runs Spark job
    # -----------------------------
    transform = BashOperator(
        task_id="extract_transform",
        bash_command=(
            'spark-submit --master local[*] --deploy-mode client '
            '--jars $(echo /opt/airflow/spark_job/jars/*.jar | tr " " ",") '
            '--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" '
            '--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" '
            '/opt/airflow/spark_job/scripts/daily_sales_summary.py'
        )
    )

    # -----------------------------
    # Validate Task
    # -----------------------------
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_output
    )

    # -----------------------------
    # Load Task (simulated)
    # -----------------------------
    load = BashOperator(
        task_id="load",
        bash_command="echo 'Loading data into Snowflake (simulated)'"
    )

    # -----------------------------
    # Task Dependencies
    # -----------------------------
    transform >> validate >> load
