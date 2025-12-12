# Snowflake Data Vault Analytics

This repository implements a Data Vault 2.0 analytics platform on Snowflake. Orchestration is handled by Apache Airflow (with Astronomer Cosmos integration) and transformations are performed with dbt Core. The infrastructure is containerized with Docker and Docker Compose.

## Architecture

The project follows a layered Data Vault architecture:

- **Staging (`models/staging`)**: Data cleansing, key hashing (MD5), and type casting.
- **Raw Vault (`models/raw_vault`)**: Incremental loading of vault objects:
    - **Hubs**: Business keys.
    - **Links**: Relationships between entities.
    - **Satellites**: Attribute history (mutable vs immutable separation).
- **Business Vault (`models/business_vault`)**: Computed satellites and business logic (e.g., effectivity satellites).
- **Marts (`models/marts`)**: BI-ready marts and reporting layers.

## Tech Stack

- **Warehouse:** Snowflake
- **Orchestration:** Apache Airflow (tested with Airflow 2.10) + Astronomer Cosmos
- **Transformations:** dbt Core (tested with v1.8)
- **Environment:** Docker & Docker Compose
- **Package manager:** `uv` (optional)
- **CI/CD:** GitHub Actions
- **SQL linting:** SQLFluff

## Project Structure

```text
.
├── airflow/                 # Airflow configs and plugins
│   └── dags/                # DAG definitions (including Cosmos integration)
├── dbt_project/             # dbt project: models, seeds, tests
├── .github/workflows/       # CI/CD pipelines
├── docker-compose.yaml      # Docker Compose stack
└── Makefile                 # Convenience commands
```

## Installation and Run

### 1. Prerequisites

- Docker Desktop
- Git
- (Optional) Make
- Snowflake account and credentials

### 2. Configuration

Copy the example environment file and fill in your Snowflake credentials:

```bash
cp .env.example .env
```

Fill in the `.env` values for `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc.

### 3. Start the platform

Build and start the containers:

```bash
make up
# Or manually:
docker-compose up -d --build
```

### 4. Web UIs

- Airflow UI: http://localhost:8080 (default creds may be `airflow` / `airflow`)
- dbt documentation server (after docs generation): http://localhost:8001

## Usage (Makefile)

Common commands provided via the `Makefile`:

| Command | Description |
| :--- | :--- |
| `make up` | Build and start the environment |
| `make down` | Stop and remove containers |
| `make logs` | Tail Airflow logs |
| `make lint` | Run SQLFluff linter |
| `make fix` | Auto-fix SQLFluff issues |
| `make dbt-full` | Install deps, run seeds, run models, run tests |
| `make dbt-run` | Run dbt models only |
| `make dbt-test` | Run dbt tests |
| `make dbt-docs-gen` | Generate dbt documentation |
| `make dbt-docs-serve` | Serve generated dbt docs |

## CI/CD

The repository contains a GitHub Actions workflow (`.github/workflows/ci.yml`) that runs on pull requests and typically performs:

1. SQL linting with SQLFluff (Snowflake dialect).
2. dbt environment checks (`dbt debug`).
3. dbt parsing and optional `dbt run --full-refresh` for validation in CI.
4. dbt tests (`dbt test`).

## Notes

- Update your `.env` and `profiles.yml` as needed for local development or CI.
- Adjust Airflow and dbt versions in the Dockerfile/pyproject if you require different versions.

If you want, I can also add an English README badge section and example `.env` snippet converted to a fenced `ini` block.