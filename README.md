# Snowflake Data Vault Analytics

![CI Status](https://github.com/<YOUR_USERNAME>/<YOUR_REPO>/actions/workflows/ci.yml/badge.svg)

Проект реализует хранилище данных архитектуры **Data Vault 2.0** на базе **Snowflake**. Оркестрация выполняется через **Apache Airflow** (с интеграцией Astronomer Cosmos), трансформация данных — через **dbt Core**. Инфраструктура полностью контейнеризирована.

## Архитектура

Проект следует многослойной архитектуре Data Vault:

* **Staging (`models/staging`):** Очистка данных, хеширование ключей (MD5), типизация.
* **Raw Vault (`models/raw_vault`):** Инкрементальная загрузка.
    * *Hubs:* Бизнес-ключи.
    * *Links:* Связи между сущностями.
    * *Satellites:* Атрибутивный состав (разделение на Mutable/Immutable).
* **Business Vault (`models/business_vault`):** Вычисляемые сателлиты и бизнес-логика (например, Effectivity Satellites).
* **Marts (`models/marts`):** Витрины данных для BI.

## Технологический стек

* **СУБД:** Snowflake
* **Оркестрация:** Airflow 2.10 + Astronomer Cosmos
* **Трансформация:** dbt Core v1.8
* **Среда:** Docker & Docker Compose
* **Управление пакетами:** uv
* **CI/CD:** GitHub Actions
* **Линтинг:** SQLFluff

## Структура проекта

```text
.
├── airflow/                 # Конфигурации Airflow
├── dags/                    # Определения DAG (включая Cosmos)
├── dbt_project/             # Основной проект dbt
│   ├── models/              # SQL логика (Staging, Vault, Marts)
│   ├── seeds/               # Справочники (CSV)
│   ├── tests/               # Тесты качества данных
│   └── dbt_project.yml      # Конфигурация dbt
├── .github/workflows/       # CI/CD пайплайны
├── Dockerfile               # Кастомный образ Airflow
├── docker-compose.yaml      # Описание сервисов
└── Makefile                 # Команды управления
```

## Установка и запуск

### 1. Предварительные требования

* Docker & Docker Compose
* Make (опционально, но рекомендуется)
* Учетная запись Snowflake

### 2. Конфигурация

Создайте файл `.env` в корне проекта на основе примера.

```bash
cp .env.example .env
```

Заполните `.env` вашими учетными данными от Snowflake (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER` и т.д.).

### 3. Запуск

Сборка образов и запуск контейнеров:

```bash
make up
# Или вручную: docker-compose up -d --build
```

### 4. Доступ к интерфейсам

* **Airflow UI:** http://localhost:8080 (Login: `airflow` / Password: `airflow`)
* **Документация dbt:** http://localhost:8001 (после запуска команды генерации)

## Использование

В проекте настроен `Makefile` для быстрого выполнения команд внутри контейнера.

| Команда | Описание |
| :--- | :--- |
| `make up` | Сборка и запуск окружения. |
| `make down` | Остановка всех контейнеров. |
| `make dbt-full` | Полный цикл: установка зависимостей -> seeds -> run -> test. |
| `make dbt-run` | Запуск только dbt моделей. |
| `make dbt-test` | Запуск тестов данных. |
| `make lint` | Проверка стиля SQL кода (SQLFluff). |
| `make dbt-docs-gen` | Генерация документации. |
| `make dbt-docs-serve` | Запуск веб-сервера документации (Lineage Graph). |

## CI/CD Pipeline

Workflow в GitHub Actions (`.github/workflows/ci.yml`) запускается при каждом Pull Request и выполняет:

1.  **Linting:** Проверка стиля кода через SQLFluff.
2.  **Connection Test:** Проверка соединения со Snowflake (`dbt debug`).
3.  **Execution:** Полная пересборка моделей (`dbt run --full-refresh`) для проверки целостности схемы.
4.  **Testing:** Запуск тестов данных (`dbt test`).