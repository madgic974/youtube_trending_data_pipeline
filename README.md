# YouTube Trending Data Pipeline

## 📝 Description

Ce projet met en œuvre un pipeline de données pour récupérer les données des vidéos tendance de l'API YouTube, les traiter en temps quasi réel et les stocker pour une analyse ultérieure. L'ensemble de l'infrastructure est conteneurisé à l'aide de Docker et orchestré avec Docker Compose.

## 🏗️ Architecture

Le flux de données passe par les composants suivants :

1.  **`youtube_producer` (Python)** : Un script Python qui récupère périodiquement les données des vidéos tendance depuis l'API YouTube v3.
2.  **`kafka` (Confluent Platform)** : Le producteur envoie les données brutes à un topic Kafka nommé `youtube_trending`. Kafka sert de courtier de messages, découplant l'ingestion du traitement des données.
3.  **`spark` (Apache Spark)** : Un job Spark consomme les données du topic Kafka. Il effectue des transformations, nettoie les données et les charge dans une base de données PostgreSQL.
4.  **`postgres` (PostgreSQL)** : Une base de données relationnelle utilisée pour stocker les données vidéo traitées et structurées.
5.  **`pgadmin` (pgAdmin 4)** : Une interface graphique web pour gérer et interroger les données stockées dans la base de données PostgreSQL.
6.  **`zookeeper`** : Nécessaire pour la gestion du cluster Kafka.

## ✨ Fonctionnalités

*   Ingestion de données depuis l'API YouTube.
*   Streaming de données résilient et évolutif avec Kafka.
*   Traitement de données distribué avec Apache Spark.
*   Stockage de données structurées dans une base de données PostgreSQL.
*   Interface web simple pour la gestion de la base de données avec pgAdmin.
*   Entièrement conteneurisé et orchestré avec Docker Compose pour une configuration facile.

## 🛠️ Stack Technique

*   **Orchestration** : Docker, Docker Compose
*   **Langage de Programmation** : Python 3.11
*   **Streaming de Données** : Apache Kafka
*   **Traitement de Données** : Apache Spark 3.5
*   **Base de Données** : PostgreSQL 15
*   **API** : YouTube Data API v3

## 🚀 Démarrage

### Prérequis

*   Docker
*   Docker Compose
*   Une clé d'API YouTube Data v3.

### Installation & Configuration

1.  **Clonez le dépôt :**
    ```bash
    git clone <votre-url-de-depot>
    cd youtube-trending-data-pipeline
    ```

2.  **Créez le fichier d'environnement :**
    Créez un fichier `.env` à la racine du projet en copiant le fichier d'exemple :
    ```bash
    cp .env.example .env
    ```
    Ensuite, remplissez le fichier `.env` avec vos informations d'identification :
    ```env
    # Clé API YouTube
    YOUTUBE_API_KEY=votre_cle_api_youtube

    # Identifiants Postgres
    POSTGRES_USER=votre_utilisateur_postgres
    POSTGRES_PASSWORD=votre_mot_de_passe_postgres
    POSTGRES_DB=votre_nom_de_db

    # Identifiants pgAdmin
    PGADMIN_DEFAULT_EMAIL=votre_email@example.com
    PGADMIN_DEFAULT_PASSWORD=votre_mot_de_passe_pgadmin
    ```

3.  **Construisez et lancez les services :**
    Utilisez Docker Compose pour construire les images et démarrer tous les conteneurs en mode détaché.
    ```bash
    docker-compose up --build -d
    ```

### Utilisation

*   **Pipeline de Données** : Le pipeline démarre automatiquement. Le service `youtube_producer` commencera à récupérer les données et à les envoyer à Kafka, et le service `spark` les traitera pour les stocker dans PostgreSQL.
*   **Accéder à pgAdmin** :
    *   Ouvrez votre navigateur web et allez sur `http://localhost:8080`.
    *   Connectez-vous avec le `PGADMIN_DEFAULT_EMAIL` et le `PGADMIN_DEFAULT_PASSWORD` que vous avez définis dans le fichier `.env`.
    *   Vous devrez ajouter une nouvelle connexion serveur pour accéder à la base de données PostgreSQL :
        *   **Host name/address** : `postgres` (le nom du service dans `docker-compose.yml`)
        *   **Port** : `5432`
        *   **Maintenance database** : La valeur de `POSTGRES_DB`
        *   **Username** : La valeur de `POSTGRES_USER`
        *   **Password** : La valeur de `POSTGRES_PASSWORD`
*   **Consulter les logs** : Pour voir les logs de tous les services, exécutez :
    ```bash
    docker-compose logs -f
    ```
    Pour suivre les logs d'un service spécifique (par exemple, `spark`) :
    ```bash
    docker-compose logs -f spark
    ```

## 📁 Structure du Projet

```
.
├── Dockerfile.producer     # Dockerfile pour le producteur Python
├── Dockerfile.spark        # Dockerfile pour le job Spark
├── docker-compose.yml      # Fichier Docker Compose pour orchestrer tous les services
├── ingestion/              # Contient le script du producteur Python
├── requirements.txt        # Dépendances Python pour le producteur
├── spark_jobs/             # Contient le script de traitement Spark
├── sql/                    # Scripts SQL pour l'initialisation de la BDD
└── README.md               # Ce fichier
```

## ⏹️ Arrêt

Pour arrêter et supprimer tous les conteneurs, réseaux et volumes créés par Docker Compose, exécutez la commande suivante depuis la racine du projet :

```bash
docker-compose down
```