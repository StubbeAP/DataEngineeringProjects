.PHONY: up down kafka-up kafka-down airflow-up airflow-down airflow-logs producer producer-logs consumer extract-api register-connector test lint clean dbt-run dbt-test kill-airflow

# Auto-detect local virtualenv; fallback to python3 if not found
PYTHON ?= $(shell if [ -f ./venv/bin/python ]; then echo ./venv/bin/python; else echo python3; fi)
DOCKER_COMPOSE ?= docker compose

# ---------------------------------------------------------------------------
# Infrastructure & Containers
# ---------------------------------------------------------------------------
up: kafka-up airflow-up

down:
	$(DOCKER_COMPOSE) down -v

kafka-up:
	$(DOCKER_COMPOSE) up -d kafka kafka-connect kafka-connect-init kafka-ui

kafka-down:
	$(DOCKER_COMPOSE) stop kafka-producer kafka-ui kafka-connect kafka

airflow-up:
	$(DOCKER_COMPOSE) up -d postgres airflow-init airflow-webserver airflow-scheduler

airflow-down:
	$(DOCKER_COMPOSE) stop airflow-webserver airflow-scheduler postgres

airflow-logs:
	$(DOCKER_COMPOSE) logs -f airflow-webserver airflow-scheduler

# ---------------------------------------------------------------------------
# Data Ingestion
# ---------------------------------------------------------------------------
producer:
	$(DOCKER_COMPOSE) up -d kafka-producer
	@echo "Kafka producer container started. Streaming live trades from Coinbase..."
	$(DOCKER_COMPOSE) logs -f kafka-producer

producer-logs:
	$(DOCKER_COMPOSE) logs -f kafka-producer

consumer:
	$(PYTHON) src/kafka/kafka_consumer.py

extract-api:
	$(PYTHON) src/kafka/api_extractor.py

register-connector:
	curl -X POST -H "Content-Type: application/json" --data @config/connect/snowflake_connector.json http://localhost:8083/connectors

# ---------------------------------------------------------------------------
# dbt Transformations
# ---------------------------------------------------------------------------
dbt-run:
	cd src/transform && dbt run --profiles-dir .

dbt-test:
	cd src/transform && dbt test --profiles-dir .

# ---------------------------------------------------------------------------
# Testing & Quality
# ---------------------------------------------------------------------------
test:
	PYTHONPATH=. pytest tests/

lint:
	sqlfluff lint src/transform/models/ --dialect snowflake

clean:
	rm -rf crypto_trades.duckdb*
	rm -rf __pycache__
	rm -rf src/*/__pycache__
	rm -rf src/*/*/__pycache__
	rm -rf tests/__pycache__
	rm -rf src/transform/target
	rm -rf src/transform/dbt_packages

kill-airflow:
	pkill -9 -f "airflow" 2>/dev/null || true
	@echo "All local Airflow processes terminated and ports freed."
