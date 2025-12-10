<!-- omit in toc -->
<h1 align="center">Backend YouTube Trending Data Pipeline</h1>

This project is a personal initiative to design, build, and deploy a complete data engineering pipeline. It demonstrates key skills in data orchestration, distributed processing, and system architecture using modern data stack technologies. The goal is to process data from the YouTube API and make it available for analytics, showcasing a practical application of data engineering principles.

As an aspiring Data Engineer, I undertook this project to bridge the gap between theory and practice. My primary objective is to expand my technical skillset by mastering industry-standard tools such as Kafka, Airflow, and Docker. This initiative reflects my dedication to continuous learning and my drive to solve complex data challenges using modern infrastructure.

**Core Competencies Demonstrated:**
- **Data Orchestration:** Scheduling, executing, and monitoring complex data workflows with **Apache Airflow**.
- **Distributed Data Processing:** Transforming and loading data in a scalable manner using **Apache Spark**.
- **Data Ingestion & Messaging:** Building a resilient ingestion service and decoupling system components with **Python** and **Apache Kafka**.
- **Containerization & IaC:** Defining and managing a multi-service application using **Docker** and **Docker Compose**, embodying Infrastructure as Code principles.
- **System Architecture:** Designing an end-to-end batch processing pipeline, from API polling to database storage.
- **Database Management:** Storing structured data in a relational database (**PostgreSQL**) and writing efficient `UPSERT` queries.

---

## 🏗️ Architecture & Data Flow

The entire pipeline is orchestrated by Apache Airflow. A DAG is scheduled to run every 30 minutes, executing the following sequence of tasks:

1.  **Orchestration (`Apache Airflow`)**:
    - A DAG (`youtube_data_pipeline`) is scheduled to run every 30 minutes.
    - Airflow manages task dependencies, retries, and logging.

2.  **Task 1: Data Ingestion (`Python` & `DockerOperator`)**:
    - Airflow's `DockerOperator` launches a container from a dedicated Python image.
    - This ephemeral container runs a Python script that fetches the most popular videos in France from the YouTube v3 API.
    - The raw data (JSON) is then published to a Kafka topic named `youtube_trending`.

3.  **Data Buffering (`Apache Kafka`)**:
    - Kafka acts as a resilient message broker, decoupling the ingestion process from the transformation process. This allows services to operate independently.

4.  **Task 2: Data Transformation (`Spark` & `DockerOperator`)**:
    - Upon successful completion of the ingestion task, Airflow triggers a second `DockerOperator`.
    - This operator launches a container with Apache Spark.
    - The Spark job reads all available data from the `youtube_trending` Kafka topic in a batch process (`read` from earliest to latest offsets).
    - It performs transformations (schema enforcement, data type casting) and deduplication.

5.  **Data Storage (`PostgreSQL`)**:
    - The transformed data is loaded into a PostgreSQL database.
    - An `INSERT ... ON CONFLICT DO UPDATE` (UPSERT) statement is used to efficiently insert new videos and update existing ones.


## 🛠️ Tech Stack

| Category          | Technology                                      |
| ----------------- | ----------------------------------------------- |
| **Orchestration** | Apache Airflow 2.9                              |
| **Containerization**| Docker, Docker Compose                          |
| **Data Processing** | Apache Spark 3.5                                |
| **Messaging**     | Apache Kafka (Confluent Platform 7.5)           |
| **Database**      | PostgreSQL 15                                   |
| **Programming**   | Python 3.11                                     |
| **API**           | YouTube Data API v3                             |
| **DB Management** | pgAdmin 4                                       |

## 🚀 Start

### Prérequis

*   Docker
*   Docker Compose
*   A YouTube Data API v3 Key.

### Installation & Configuration

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd youtube-trending-data-pipeline
    ```

2.  **Create the environment file:**
    Create a `.env` file at the project root.
    ```bash
    cp .env.example .env
    ```
    Then, fill the `.env` file with your credentials. It must include credentials for both the application's database and Airflow's metadata database.
    ```env
    # YouTube API Key
    YOUTUBE_API_KEY

    # Application Postgres Credentials
    POSTGRES_USER
    POSTGRES_PASSWORD
    POSTGRES_DB

    # pgAdmin Credentials
    PGADMIN_DEFAULT_EMAIL
    PGADMIN_DEFAULT_PASSWORD

    # Airflow Postgres Credentials
    AIRFLOW_POSTGRES_USER
    AIRFLOW_POSTGRES_PASSWORD
    AIRFLOW_POSTGRES_DB

    # Airflow User ID (to avoid permission issues with Docker socket)
    AIRFLOW_UID=50000
    ```

3.  **Create Airflow directories:**
    Airflow requires local directories for DAGs, logs, and plugins.
    ```bash
    mkdir -p dags logs plugins
    ```

4.  **Build and launch the services:**
    Use Docker Compose to build all custom images and start the containers in detached mode.
    ```bash
    docker-compose up --build -d
    ```

### Utilisation

1.  **Access the Airflow UI:**
    - Open your browser and navigate to `http://localhost:8081`.
    - Log in with the default credentials: `admin` / `admin`.

2.  **Configure Airflow Variables:**
    - The DAG requires API keys and credentials to be stored as Airflow Variables for security.
    - In the Airflow UI, go to **Admin -> Variables**.
    - Create the following variables:
      - `YOUTUBE_API_KEY`: Your YouTube API key.
      - `KAFKA_BROKER`: `kafka:9092`
      - `POSTGRES_USER`: Your application's Postgres user.
      - `POSTGRES_PASSWORD`: Your application's Postgres password.
      - `POSTGRES_DB`: Your application's Postgres database name.

3.  **Enable and Trigger the DAG:**
    - On the main DAGs page, find `youtube_data_pipeline`.
    - Unpause the DAG using the toggle on the left.
    - The pipeline will now run automatically every 30 minutes. You can also trigger it manually by clicking the "Play" button on the right.

4.  **Access pgAdmin:**
    - Navigate to `http://localhost:8080`.
    - Log in with the `PGADMIN_DEFAULT_EMAIL` and `PGADMIN_DEFAULT_PASSWORD` from your `.env` file.
    - Add a new server connection to access the application database:
        - **Host name/address**: `postgres` (the service name in `docker-compose.yml`)
        - **Port**: `5432`
        - **Maintenance database**: The value of `POSTGRES_DB`
        - **Username**: The value of `POSTGRES_USER`
        - **Password**: The value of `POSTGRES_PASSWORD`

5.  **Monitor Logs:**
    To view the logs for all running services:
    ```bash
    docker-compose logs -f
    ```
    To follow the logs of a specific service (e.g., `airflow-scheduler`):
    ```bash
    docker-compose logs -f airflow-scheduler
    ```

## ⏹️ Stop

```bash
docker-compose down
```