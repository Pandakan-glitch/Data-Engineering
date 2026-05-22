from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
    'retries': 0,
}

with DAG(
    dag_id='streaming_sales_etl',
    default_args=default_args,
    description='Micro-batch ETL from Kafka to PostgreSQL',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@continuous',
    catchup=False,
    max_active_runs=1,
    tags=['sales', 'streaming', 'spark'],
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_streaming',
        application='/opt/airflow/Etl/Etl_stream.py',
        conn_id='spark_default',
        
        conf={
            'spark.master': 'spark://spark:7077',
            'spark.submit.deployMode': 'client',
            'spark.driver.cores': '1',
            'spark.driver.memory': '1g',
            'spark.executor.cores': '1',
            'spark.executor.memory': '1g',
            'spark.executor.instances': '1',
            'spark.streaming.backpressure.enabled': 'true',
            'spark.streaming.kafka.maxRatePerPartition': '100',
            'spark.sql.streaming.checkpointLocation': '/opt/spark-apps/checkpoint',
            # ✅ ADD THESE TO FIX PYTHON VERSION MISMATCH
            'spark.pyspark.python': '/usr/bin/python3',
            'spark.pyspark.driver.python': '/usr/bin/python3',
        },
        
        application_args=[
            '--kafka-bootstrap-servers', 'kafka:9092',
            '--postgres-url', 'jdbc:postgresql://postgres:5432/Test_Tb',
            '--postgres-user', 'postgres',
            '--postgres-password', 'Mics0123',
            '--topic', 'sales_topic',
        ],
        
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.10',
        
        verbose=True,
    )