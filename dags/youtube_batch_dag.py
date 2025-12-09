from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    "youtube_batch_processing",
    start_date=datetime(2025, 10, 29),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    spark_batch_job = SparkSubmitOperator(
        task_id="spark_transform_youtube_batch",
        application="/opt/spark/work-dir/spark_jobs/batch_transform.py",
        packages="org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        conn_id="spark_default",
        verbose=True
    )
