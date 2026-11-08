.PHONY: up down kafka-up kafka-down airflow-up airflow-down airflow-logs producer consumer extract-api register-connector test lint clean dbt-run dbt-test kill-airflow

DOCKER_COMPOSE ?= docker compose

# ---------------------------------------------------------------------------
# Infrastructure & Containers
# ---------------------------------------------------------------------------
up: kafka-up airflow-up

down:
	$(DOCKER_COMPOSE) down -v

kafka-up:
	$(DOCKER_COMPOSE) up -d kafka kafka-connect kafka-ui

kafka-down:
	$(DOCKER_COMPOSE) stop kafka-ui kafka-connect kafka

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
	python src/kafka/main.py

consumer:
	python src/kafka/kafka_consumer.py

extract-api:
	python src/kafka/api_extractor.py

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
