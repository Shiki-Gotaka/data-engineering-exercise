from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
import pandas as pd
import psycopg2
import os
from error_handling import (
    notify_failure,
    notify_success,
    notify_retry
)

# Default DAG arguments
default_args = {
    'owner': 'data_engineer',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'sales_data_pipeline',                                      # DAG name
    # Default DAG arguments
    default_args=default_args,
    description='Basic ETL pipeline with dbt transformation',
    schedule='@daily',                                 # Run the DAG daily
    start_date=datetime(2024, 1, 20),                           # Start date
    # Disable catchup (run only for today's date)
    catchup=False,
)

# Task 1: Load CSV to PostgreSQL


def load_csv_to_postgres():
    import logging
    """Load CSV file to raw.sales_data table"""
    # Change this path to your actual CSV location
    csv_path = r'/workspaces/Data Engineer training/sales_sample.csv'
    logger = logging.getLogger("airflow.task")

    df = pd.read_csv(csv_path)

    try:
        logger.info('Start loading CSV to Postgres')
        conn = psycopg2.connect(
            host='localhost',
            database='data_engineering_exercise',
            user='postgres',
            password='postgres',  # Change this to your PostgreSQL password
            port=5432
        )

        cursor = conn.cursor()

        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO raw.sales_data 
            (product_name, category, price, quantity_sold, sale_date, region)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, tuple(row))

        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Loaded {len(df)} rows to PostgreSQL")
    except Exception:
        logger.exception('Error while loading CSV to Postgres')
        raise


# Define Tasks
load_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    on_failure_callback=notify_failure,
    on_success_callback=notify_success,
    on_retry_callback=notify_retry,
    dag=dag,
)

# Task 2: Run dbt models
dbt_run_task = BashOperator(
    task_id='run_dbt_models',
    bash_command="cd '/workspaces/Data Engineer training/sales_analytics' && dbt run --profiles-dir .",
    on_failure_callback=notify_failure,
    on_success_callback=notify_success,
    on_retry_callback=notify_retry,
    dag=dag,
)

# Task 3: Run dbt tests (optional)
dbt_test_task = BashOperator(
    task_id='run_dbt_tests',
    bash_command="cd '/workspaces/Data Engineer training/sales_analytics' && dbt test --profiles-dir .",
    on_failure_callback=notify_failure,
    on_success_callback=notify_success,
    on_retry_callback=notify_retry,
    dag=dag,
)

# Set task dependencies
load_task >> dbt_run_task >> dbt_test_task
