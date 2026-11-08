# Crypto Kafka-Snowflake Streaming Pipeline

A real-time data engineering pipeline that streams live cryptocurrency trade data from Coinbase, buffers it through Apache Kafka, sinks it into Snowflake, and orchestrates dbt transformations using Apache Airflow + Astronomer Cosmos — following a **Medallion Architecture** (Bronze → Silver → Gold).

---

## Architecture

```
Coinbase WebSocket
      │
      ▼
  Kafka Producer  ──►  Kafka Topic (raw-crypto-trades)
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
            Kafka Connect          Airflow + Cosmos
         (Snowflake Sink)         (Hourly Scheduler)
                    │                    │
                    ▼                    ▼
            Snowflake Bronze       1_Bronze (stg_crypto_trades)
                               ──► 2_Silver (dim_products, dim_date, fct_crypto_trades)
                               ──► 3_Gold  (hourly_crypto_summary)
```

---

## Project Structure

```
├── Dockerfile                    # Custom Airflow image (dbt-snowflake + Cosmos)
├── docker-compose.yml            # Full stack: Kafka, Kafka Connect, Airflow, Postgres
├── Makefile                      # All CLI commands
├── config/
│   ├── connect/                  # Kafka Connect Snowflake sink config
│   └── ingestion/                # Kafka producer config (topics, Coinbase URLs)
└── src/
    ├── kafka/                    # Streaming ingestion modules
    │   ├── main.py               # Producer entrypoint
    │   ├── kafka_producer.py     # Kafka producer client
    │   ├── websocket_client.py   # Coinbase WebSocket listener
    │   ├── kafka_consumer.py     # Kafka consumer
    │   ├── api_extractor.py      # REST API batch extractor
    │   ├── config.py             # Config loader
    │   └── logger.py             # Centralized logger
    ├── dags/                     # Airflow DAGs
    │   ├── crypto_analytics_dag.py  # Hourly Cosmos dbt pipeline
    │   └── backfill_dag.py          # Parameterized historical backfill
    ├── transform/                # dbt project (Snowflake)
    │   └── models/
    │       ├── 1_bronze/         # stg_crypto_trades (view)
    │       ├── 2_silver/         # dim_products, dim_date, fct_crypto_trades
    │       └── 3_gold/           # hourly_crypto_summary
    └── infra/                    # Terraform (Snowflake warehouse, roles, schemas)
```

---

## Services & Ports

| Service             | URL                                        | Credentials       |
|:--------------------|:-------------------------------------------|:------------------|
| Airflow Webserver   | [http://localhost:8088](http://localhost:8088) | `admin` / `admin` |
| Kafka UI            | [http://localhost:8080](http://localhost:8080) | —                 |
| Kafka Connect API   | [http://localhost:8083](http://localhost:8083) | —                 |
| Kafka Broker        | `localhost:29092`                          | —                 |
| Postgres (Airflow)  | `localhost:5432`                           | `airflow` / `airflow` |

---

## Quickstart

### Prerequisites
- Docker Desktop (v4+)
- Python 3.11+

### 1. Set up environment
```bash
cp .env.example .env
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

---

## Starting Kafka

Start the Kafka stack (Broker, Kafka Connect, Kafka UI):
```bash
make kafka-up
```

Start the live Coinbase trade producer:
```bash
make producer
```

Register the Snowflake Kafka Connector:
```bash
make register-connector
```

Stop Kafka containers:
```bash
make kafka-down
```

---

## Starting Airflow

```bash
# Start Airflow (Postgres + init + webserver + scheduler)
make airflow-up

# Stream live logs
make airflow-logs

# Open the UI → http://localhost:8088
# Login: admin / admin

# Stop Airflow containers
make airflow-down
```

---

## dbt Transformations

Run or test dbt models manually (without Airflow):
```bash
make dbt-run
make dbt-test
```

Lint SQL models:
```bash
make lint
```

---

## Troubleshooting

```bash
# Kill any stuck local Airflow processes
make kill-airflow

# Clean build artifacts and caches
make clean
```
