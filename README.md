# Snowflake Data Vault Project

This repository implements a **Data Vault 2.0** architecture on Snowflake, orchestrated by **Apache Airflow** and **Astronomer Cosmos**.

## Architecture Overview

The project follows a multi-layer Data Vault architecture:

1.  **Staging:** Raw data ingestion and hashing (MD5).
2.  **Raw Vault:** Hubs, Links, and Satellites (including Split Satellites for variable/invariant attributes).
3.  **Business Vault:** Point-in-Time (PIT) tables and computed satellites (e.g., Effectivity Satellites).
4.  **Marts:** Dimensional modeling (Star Schema) for analytics.

## Tech Stack

* **Data Warehouse:** Snowflake
* **Orchestration:** Apache Airflow (via Astronomer Cosmos)
* **Transformation:** dbt Core (v1.7+)
* **Language:** Python 3.10+
* **Linting:** SQLFluff

## Configuration

1.  **Environment Variables:**
    Copy the example configuration file:
    ```bash
    cp .env.example .env
    ```

2.  **Credentials:**
    Edit `.env` and fill in your Snowflake credentials:
    * `SNOWFLAKE_ACCOUNT`
    * `SNOWFLAKE_USER`
    * `SNOWFLAKE_PASSWORD`
    * `TELEGRAM_BOT_TOKEN` (Optional: for pipeline alerts)

## Usage

### Option A: Using Make (Linux / Mac / WSL)

The project includes a `Makefile` with convenience commands to manage Docker and dbt operations.

#### Prerequisites for Make

* Make installed (`brew install make` on Mac, or `apt-get install make` on Linux)
* Docker and Docker Compose running

#### Quick Start with Make

```bash
# 1. Build images (first time only)
make build

# 2. Start the project
make up

# 3. Check logs
make logs

# 4. Run dbt transformations
make dbt-full

# 5. Generate and serve documentation
make dbt-docs-gen
make dbt-docs-serve
# Then visit http://localhost:8001
```

#### Available Make Commands

| Command | Description |
| :--- | :--- |
| `make help` | Display all available commands. |
| `make build` | Build Docker images. |
| `make up` | Start all containers in detached mode. |
| `make down` | Stop and remove all containers. |
| `make restart` | Restart (equivalent to `make down` + `make up`). |
| `make logs` | Stream logs from Airflow scheduler. |
| `make dbt-full` | Run full dbt build (seeds → run → test). |
| `make dbt-deps` | Install dbt dependencies. |
| `make dbt-seed` | Load seed data. |
| `make dbt-run` | Run dbt models only. |
| `make dbt-test` | Run dbt tests. |
| `make dbt-docs-gen` | Generate dbt documentation. |
| `make dbt-docs-serve` | Serve dbt docs locally at `http://localhost:8001`. |
| `make lint` | Lint SQL code with SQLFluff. |
| `make clean` | Clean dbt artifacts and dependencies. |
| `make bash` | Open a bash shell in the scheduler container. |

### Option B: Using Docker Compose (Windows / PowerShell)

If `make` is not available, use the following commands directly in PowerShell/CMD:

**1. Build and Start**
```powershell
docker compose build
docker compose up -d
```
Access Airflow UI at http://localhost:8080 (User: airflow, Pass: airflow)

### 2. Stop Project
```powershell
docker compose down
```

### 3. Run dbt Manually (Inside Container)
```bash
docker compose exec airflow-scheduler dbt build --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
```

### 4. Generate Documentation
```bash
# Generate docs
docker compose exec airflow-scheduler dbt docs generate --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project

# Serve docs (then visit http://localhost:8001)
docker compose run --rm -p 8001:8080 --entrypoint "dbt docs serve --port 8080 --address 0.0.0.0 --no-browser --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project" airflow-scheduler
```

## Development & Linting

SQL linting is configured via `sqlfluff` using the Jinja templater (offline mode).

**Prerequisites:**
* `uv` (or pip)

**Run Linter:**

```bash
# Sync dependencies
uv sync

# Lint all models
uv run sqlfluff lint dbt_project/models
```
