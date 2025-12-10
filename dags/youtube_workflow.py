from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# --- Configuration ---
# Le nom du projet docker-compose. Par défaut, c'est le nom du dossier
# contenant votre docker-compose.yml, en minuscules.
# Ex: 'youtube-trending-data-pipeline'
PROJECT_NAME = "youtube-trending-data-pipeline"

# Le nom du réseau créé par docker-compose.
# Vérifiez le nom exact avec la commande `docker network ls`
NETWORK_NAME = f"{PROJECT_NAME}_app-network"

# Les noms des images construites par docker-compose
PRODUCER_IMAGE = f"{PROJECT_NAME}-youtube_producer"
SPARK_IMAGE = f"{PROJECT_NAME}-spark"


with DAG(
    dag_id="youtube_data_pipeline",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    description="Pipeline d'ingestion et de transformation des données YouTube.",
    schedule="*/30 * * * *",  # Exécution toutes les 30 minutes
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["youtube", "docker", "spark"],
) as dag:
    # --- Tâche 1: Lancer le producteur Python pour ingérer les données ---
    ingest_data = DockerOperator(
        task_id="ingest_youtube_data",
        image=PRODUCER_IMAGE,
        command=["python", "ingestion/youtube_producer.py"],
        network_mode=NETWORK_NAME,
        auto_remove=True,
        mount_tmp_dir=False,
        do_xcom_push=False,
        # Passe les variables d'environnement nécessaires au conteneur
        # Celles-ci doivent être configurées comme "Variables" dans l'UI d'Airflow
        environment={
            "YOUTUBE_API_KEY": "{{ var.value.YOUTUBE_API_KEY }}",
            "KAFKA_BROKER": "{{ var.value.KAFKA_BROKER }}",
        },
    )

    # --- Tâche 2: Lancer le job Spark pour transformer les données ---
    transform_data = DockerOperator(
        task_id="transform_spark_data",
        image=SPARK_IMAGE,
        # Commande pour lancer le script Spark en mode batch
        command=[
            "/opt/spark/bin/spark-submit",
            "--master", "local[*]",
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0",
            "/opt/spark/work-dir/spark_jobs/batch_transform.py",
        ],
        network_mode=NETWORK_NAME,
        auto_remove=True,
        mount_tmp_dir=False,
        do_xcom_push=False,
        # Passe les variables pour la connexion à Kafka et Postgres
        environment={
            "KAFKA_BROKER": "{{ var.value.KAFKA_BROKER }}",
            "POSTGRES_USER": "{{ var.value.POSTGRES_USER }}",
            "POSTGRES_PASSWORD": "{{ var.value.POSTGRES_PASSWORD }}",
            "POSTGRES_DB": "{{ var.value.POSTGRES_DB }}",
        },
    )

    # --- Définition des dépendances ---
    # La tâche de transformation ne commence que si l'ingestion a réussi
    ingest_data >> transform_data