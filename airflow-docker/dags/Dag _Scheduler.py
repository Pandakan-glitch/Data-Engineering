import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import \
    PostgresHook  
from pyspark.sql import SparkSession

# 👇 This adds your project folder to Python path so it can import the ETL code
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Etl.Etl_PySpark import run_etl_for_date  # your ETL function

# ✅ DAG default settings
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ✅ Wrapper function to add DB connection
def run_etl_with_db(**context):
    # Create SparkSession
    spark = (
        SparkSession.builder
        .appName("daily_sales_etl")
        .master("local[1]")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", "localhost")
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.10.jar")
        .getOrCreate()
    )

    execution_date = context["execution_date"]  # datetime object from Airflow

    # Call ETL
    run_etl_for_date(spark, execution_date, force_from_raw=True)



# ✅ Define the DAG
with DAG(
    dag_id='daily_sales_etl',
    default_args=default_args,
    description='ETL that loads daily sales data into PostgreSQL',
    start_date=datetime(2023, 1, 1),  # Fixed: set start_date to past date
    schedule_interval='@daily',  # Run every day
    catchup=False,               # Don't backfill missed days
    tags=['sales', 'etl'],
) as dag:
    
    etl_task = PythonOperator(
        task_id='run_sales_etl',
        python_callable=run_etl_with_db,
    )                     