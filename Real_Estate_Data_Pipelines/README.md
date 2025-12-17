# Real Estate Data Pipelines Setup

This repository contains the **web scraping, ETL, orchestration, and Vector Database (VectorDB) pipelines** used to collect, process, store, and index real estate data for analytics and ML/LLM use cases.

The stack includes:

* **Dagster** for orchestration
* **PostgreSQL** for Dagster metadata storage
* **Google BigQuery** for analytics (raw + mart layers)
* **Milvus** for vector storage (embeddings)
* **Grafana** for monitoring
* **Prometheus** for metrics collection
* **Docker Compose** for local deployment

---

## Prerequisites

Make sure you have the following installed locally:

* Docker & Docker Compose
* Git
* Nano or any text editor
* A Google Cloud project with BigQuery enabled

---

## Run the Pipeline

### 1. Navigate to the Configs directory

```bash
cd Configs
````

---

### 2. Create the Google BigQuery service account file

Copy the example file:

```bash
cp big_query_service_account.json.example big_query_service_account.json
```

Edit the file:

```bash
nano big_query_service_account.json
```

Fill in your **Google Cloud service account credentials**:

```json
{
  "type": "service_account",
  "project_id": "",
  "private_key_id": "",
  "private_key": "",
  "client_email": "",
  "client_id": "",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "",
  "universe_domain": "googleapis.com"
}
```

⚠️ **Never commit this file to Git.**

---

### 3. Add pipeline-level configurations

Edit the same config file to include pipeline parameters:

```json
{
  "GCP_PROJECT_ID": "",
  "BQ_RAW_DATASET_ID": "",
  "BQ_RAW_TABLE_ID": "",
  "BQ_MART_DATASET_ID": "",
  "BQ_MART_TABLE_ID": "",

  "MAX_PAGES": 10,
  "LOG_DIR": "logs/",

  "_comment_milvus": "Milvus Vector Database Configuration",
  "MILVUS_HOST": "",
  "MILVUS_PORT": "",
  "MILVUS_COLLECTION_NAME": "",

  "EMBEDDING_MODEL": "paraphrase-multilingual-MiniLM-L12-v2",
  "EMBEDDING_DIM": 384,
  "BATCH_SIZE": 1000,

  "_comment_generation": "Used for text similarity / retrieval (not mandatory for automation)",
  "GENERATION_MODEL": "paraphrase-multilingual-MiniLM-L12-v2",

  "_comment_alternatives": "Alternative embedding models with stronger Arabic support",
  "_comment_alternative_models": [
    "sentence-transformers/paraphrase-multilingual-mpnet-base-v2",
    "sentence-transformers/distiluse-base-multilingual-cased-v2",
    "all-MiniLM-L6-v2"
  ],

  "_comment_aws": "AWS S3 Credentials",
  "AWS_ACCESS_KEY_ID": "",
  "AWS_SECRET_ACCESS_KEY": "",
  "AWS_REGION": "" 
}
```

---

### 4. Create the main pipeline config file

```bash
cp Real_Estate_Data_Pipelines.json.example Real_Estate_Data_Pipelines.json
```

---

### 5. Edit pipeline-specific settings

```bash
nano Real_Estate_Data_Pipelines.json
```

Use this file to control:

* Scraping sources
* Ingestion parameters
* ETL behavior
* Feature extraction options

---

### 6. Move to Docker environment configuration

```bash
cd ../Real_Estate_Data_Pipelines/docker/env
```

---

### 7. Create environment variable files

```bash
cp .env.example.dagster .env.dagster
cp .env.example.grafana .env.grafana
cp .env.example.postgres .env.postgres
cp .env.example.dagster-postgres-exporter .env.dagster-postgres-exporter
```

Edit each file as needed:

#### Dagster

```bash
nano .env.dagster
```

```env
DAGSTER_HOME=/opt/dagster/dagster_home
DAGSTER_POSTGRES_HOST=postgres
DAGSTER_POSTGRES_DB=dagster
DAGSTER_POSTGRES_USER=dagster
DAGSTER_POSTGRES_PASSWORD=password
```

#### Grafana

```bash
nano .env.grafana
```

```env
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=admin_password
GF_USERS_ALLOW_SIGN_UP=false
GF_SERVER_HTTP_PORT=3000
GF_INSTALL_PLUGINS=grafana-piechart-panel
```

#### PostgreSQL

```bash
nano .env.postgres
```

```env
POSTGRES_USER=dagster
POSTGRES_PASSWORD=password
POSTGRES_DB=database
```

#### PostgreSQL Exporter

```bash
nano .env.dagster-postgres-exporter
```

```env
DATA_SOURCE_URI=dagster-postgres:5432/dagster?sslmode=disable
DATA_SOURCE_USER=dagster
DATA_SOURCE_PASS=dagster
```

---

### 8. Create the Dagster instance configuration

```bash
cd ../../
mkdir -p volumes/dagster
cd volumes/dagster
nano dagster.yaml
```

Example `dagster.yaml`:

```yaml
instance_class: dagster.core.instance.DagsterInstance

local_artifact_storage:
  module: dagster._core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir: /opt/dagster/dagster_home/storage

run_storage:
  module: dagster._core.storage.runs
  class: SqlRunStorage
  config:
    postgres_db:
      username: dagster
      password: password
      hostname: postgres
      db_name: dagster
      port: 5432

event_log_storage:
  module: dagster._core.storage.event_log
  class: SqlEventLogStorage
  config:
    postgres_db:
      username: dagster
      password: password
      hostname: postgres
      db_name: dagster
      port: 5432

schedule_storage:
  module: dagster._core.storage.schedules
  class: SqlScheduleStorage
  config:
    postgres_db:
      username: dagster
      password: password
      hostname: postgres
      db_name: dagster
      port: 5432

compute_logs:
  module: dagster._core.storage.compute_logs
  class: LocalComputeLogManager
  config:
    base_dir: /opt/dagster/dagster_home/compute_logs

telemetry:
  enabled: false
```

---

### 9. Build and run the Docker Compose stack

```bash
cd ../../docker
docker-compose up -d --build
```

---

## Access Services

* **Dagster UI:** [http://localhost:3000](http://localhost:3000)
* **Grafana:** [http://localhost:7000](http://localhost:7000)

---

### Grafana Dashboards

You can find preconfigured dashboards in:

```bash
cd ../../Real_Estate_BI/dashboards
```

Available dashboards:

* `milvus-dashboard.json`
* `node-exporter-dashboard.json`
* `postgres-dashboard.json`
* `prometheus-dashboard.json`

