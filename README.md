# Data Vault Platform: Airflow + dbt + Snowflake

A modern, containerized **ELT platform** built to demonstrate **Data Vault 2.0** methodology. It ingests raw data into Snowflake, transforms it using dbt, and is fully orchestrated by Apache Airflow.

![CI Status](https://img.shields.io/badge/CI-Passing-success?style=flat-square&logo=github-actions)
![Python](https://img.shields.io/badge/Python-3.10-blue?style=flat-square&logo=python)
![Docker](https://img.shields.io/badge/Docker-Enabled-blue?style=flat-square&logo=docker)

---

## Tech Stack

| Component | Technology | Description |
| :--- | :--- | :--- |
| **Orchestrator** | **Apache Airflow** | Dockerized setup. Uses `astronomer-cosmos` to render dbt models as DAG tasks. |
| **Transformation** | **dbt Core (v1.8)** | Handles ELT logic, testing, and documentation. |
| **Warehouse** | **Snowflake** | Cloud Data Warehouse for storage and compute. |
| **Methodology** | **Data Vault 2.0** | Raw Vault modeling (Hubs, Links, Satellites). |
| **Quality & CI** | **SQLFluff** | SQL linting and auto-formatting via GitHub Actions. |
| **Manager** | **uv** | Ultra-fast Python package manager. |

---

## Architecture (Data Vault)

The pipeline transforms raw data through the following layers:

1.  **Staging (`stg_`)**: Cleans raw data and generates **MD5 Hash Keys** for Data Vault entities.
2.  **Raw Vault**:
    * **Hubs (`hub_`)**: Stores unique list of business keys (e.g., Order IDs).
    * **Links (`link_`)**: Stores relationships/transactions (e.g., Customer bought Order).
    * **Satellites (`sat_`)**: Stores descriptive attributes and history (e.g., Price, Status, Date).

---

## Quick Start (The Magic)

This project uses a `Makefile` to simplify Docker and dbt commands.

### 1. Prerequisites
* Docker Desktop (running)
* Git
* Make (optional, but recommended)

### 2. Configure Credentials
Create a `.env` file in the root directory with your Snowflake credentials:

```ini
SNOWFLAKE_ACCOUNT=xy12345
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=RAW_VAULT
DBT_PROFILES_DIR=dbt_project
```

### 3. Run the Platform
Start the entire infrastructure with one command:

```bash
make up
```

* **Airflow UI:** [http://localhost:8080](http://localhost:8080)
* **Credentials:** `admin` / `admin`

---

## Available Commands

| Command | Description |
| :--- | :--- |
| `make up` | Start Airflow and the database in the background |
| `make down` | Stop and remove all containers |
| `make logs` | View Airflow logs in real-time |
| `make lint` | Check SQL style using **SQLFluff** |
| `make fix` | Auto-fix SQL style errors |
| `make restart` | Full restart (down + up) |

---

## Project Structure

```text
.
├── airflow/               # Airflow configuration & DAGs
│   └── dags/              # dbt_pipeline.py (Cosmos integration)
├── dbt_project/           # dbt Transformation logic
│   ├── models/            # SQL Models (Hubs, Links, Sats)
│   └── seeds/             # Mock data (CSVs)
├── .github/workflows/     # CI/CD Pipelines
├── docker-compose.yaml    # Infrastructure definition
└── Makefile               # Command shortcuts
```

## CI/CD Automation

Every push to the `main` branch triggers a **GitHub Action** that:

1.  Installs dependencies via `pip`.
2.  Lints all SQL code using `sqlfluff` (Snowflake dialect).
3.  Verifies the integrity of the project using `dbt parse`.