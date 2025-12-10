# Snowflake Analytics (Data Vault 2.0)

This project implements a Data Warehouse using **Data Vault 2.0** architecture, orchestrated by **Airflow** and transformed using **dbt** (Data Build Tool) on **Snowflake**.

## Project Structure

The project follows the standard Data Vault 2.0 layers:

* **Staging (`models/staging`):** View layer. Responsible for column aliasing, data type casting, and HashDiff calculation.
* **Raw Vault (`models/raw_vault`):** Incremental tables.
    * **Hubs & Links:** Business keys and relationships.
    * **Satellites:** Split into *Immutable* (e.g., dates, priority) and *Mutable* (e.g., status, price) data to optimize storage and history tracking.
* **Marts (`models/marts`):** Presentation layer (Facts & Dimensions) for BI tools.

## Prerequisites

* Docker & Docker Compose
* Snowflake Account
* Make (optional, for using Makefile shortcuts)

## Setup & Installation

### 1. Environment Configuration
Create a `.env` file from the example template. This file will store your sensitive credentials and is excluded from Git.

```bash
cp .env.example .env
```

**Important:** Open `.env` and fill in your Snowflake credentials (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc.) and Airflow settings.

### 2. Start Services
Build and start the Airflow container in detached mode:

```bash
make up
# Or standard docker command:
# docker-compose up -d --build
```
### 4. Access Airflow
Once started, access the Airflow UI:

* **URL:** http://localhost:8080
* **Credentials:** See `_AIRFLOW_WWW_USER_USERNAME` / `PASSWORD` in your `.env` file.

## Development

### Linting
We use **SQLFluff** to ensure code quality and style consistency.

```bash
make lint
```

### Testing
Run dbt data tests:

```bash
make dbt-test
```

## CI/CD
GitHub Actions workflow is configured in `.github/workflows/dbt_ci.yml` to run linting and models on pull requests. It uses GitHub Secrets for Snowflake authentication.