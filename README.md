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
* **Language:** Python 3.10+ (managed via `uv`)
* **Linting:** SQLFluff
* **Alerting:** Telegram Bot (Real-time success/failure notifications)

## Configuration

### 1. Environment
Copy the example env file and edit it:

```bash
cp .env.example .env
```

### 2. Credentials
Edit `.env` and fill in your Snowflake credentials and optional Telegram fields. Example:

```ini
# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# Optional: Telegram alerts
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

## Usage

You can run the project with `make` (Linux / Mac / WSL) or using Docker Compose directly (Windows / PowerShell).

### Option A — Using `make`

Quick start with `make`:

```bash
# Build images (first run)
make build

# Start services in background
make up

# Tail Airflow scheduler logs
make logs

# Run full dbt build (seeds -> models -> tests)
make dbt-full

# Generate and serve dbt docs
make dbt-docs-gen
make dbt-docs-serve
```

Common Makefile commands (see `Makefile` for exact targets):

| Command | Description |
| :--- | :--- |
| `make help` | Show available commands |
| `make build` | Build Docker images |
| `make up` | Start containers (detached) |
| `make down` | Stop and remove containers |
| `make logs` | Stream Airflow scheduler logs |
| `make dbt-full` | Run full dbt build |
| `make dbt-run` | Run dbt models only |
| `make dbt-test` | Run dbt tests |
| `make dbt-docs-gen` | Generate dbt docs |
| `make dbt-docs-serve` | Serve dbt docs locally |
| `make lint` | Run SQLFluff linter |

### Option B — Using Docker Compose (PowerShell)

```powershell
docker compose build
docker compose up -d

# Stop services
docker compose down

# Run dbt inside the scheduler container
docker compose exec airflow-scheduler dbt build --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project

# Generate docs
docker compose exec airflow-scheduler dbt docs generate --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project

# Serve docs (visit http://localhost:8001)
docker compose run --rm -p 8001:8080 --entrypoint "dbt docs serve --port 8080 --address 0.0.0.0 --no-browser --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project" airflow-scheduler
```

**Airflow UI:** http://localhost:8080 (default user/password: `airflow` / `airflow`)

## Development & Linting

SQL linting is configured via `sqlfluff` with the Jinja templater. Example:

```bash
# Sync dependencies
uv sync

# Lint all models
uv run sqlfluff lint dbt_project/models
```
