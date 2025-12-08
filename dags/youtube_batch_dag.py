from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    "youtube_streaming",
    start_date=datetime(2025, 10, 29),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    spark_stream = SparkSubmitOperator(
        task_id="spark_consume_youtube",
        application="/opt/spark_jobs/streaming_processor.py",
        conn_id="spark_default",
        verbose=True
    )
