# Real Estate Data Pipelines Setup

This folder contains the **web scraping, ETL, and VectorDB pipelines**.

---

## Run the Pipeline

### 1. Navigate to the Configs directory

```bash
cd Configs
```

### 2. Create Google BigQuery service account file

```bash
cp big_query_service_account.json.example big_query_service_account.json
```

### 3. Add your service account configurations

```bash
nano big_query_service_account.json
```

Fill in your Google service account credentials.

---

### 4. Create Real Estate Data Pipelines config file

```bash
cp Real_Estate_Data_Pipelines.json.example Real_Estate_Data_Pipelines.json
```

### 5. Add your pipeline configurations

```bash
nano Real_Estate_Data_Pipelines.json
```

Update with your project-specific configurations.

---

### 6. Move to the Docker env folder

```bash
cd ../Real_Estate_Data_Pipelines/docker/env
```

### 7. Update Dagster, Postgres, and Grafana environment configurations

```bash
cp .env.example.dagster .env.dagster
cp .env.example.grafana .env.grafana
cp .env.example.postgres .env.postgres
```

Edit the `.env` files:

```bash
nano .env.dagster
nano .env.grafana
nano .env.postgres
```

Replace placeholders with your local credentials and configuration.

---

### 8. Create `dagster.yaml`

```bash
cd ../../
mkdir -p volumes/dagster
cd volumes/dagster
nano dagster.yaml
```

Populate `dagster.yaml` with the following template (replace placeholders with your actual paths and credentials):

```yaml
instance_class: dagster.core.instance.DagsterInstance

local_artifact_storage:
  module: dagster._core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir: /path/to/dagster_home/storage

run_storage:
  module: dagster._core.storage.runs
  class: SqlRunStorage
  config:
    postgres_db:
      username: <DB_USERNAME>
      password: <DB_PASSWORD>
      hostname: <DB_HOSTNAME>
      db_name: <DB_NAME>
      port: 5432

event_log_storage:
  module: dagster._core.storage.event_log
  class: SqlEventLogStorage
  config:
    postgres_db:
      username: <DB_USERNAME>
      password: <DB_PASSWORD>
      hostname: <DB_HOSTNAME>
      db_name: <DB_NAME>
      port: 5432

schedule_storage:
  module: dagster._core.storage.schedules
  class: SqlScheduleStorage
  config:
    postgres_db:
      username: <DB_USERNAME>
      password: <DB_PASSWORD>
      hostname: <DB_HOSTNAME>
      db_name: <DB_NAME>
      port: 5432

compute_logs:
  module: dagster._core.storage.compute_logs
  class: LocalComputeLogManager
  config:
    base_dir: /path/to/dagster_home/compute_logs

telemetry:
  enabled: false
```

---

### 9. Build and run the Docker Compose stack

```bash
cd ../../docker
docker-compose up -d --build
```

This will build all services and start the pipeline in detached mode.
