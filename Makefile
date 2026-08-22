.PHONY: up down producer consumer query register-connector test-local clean

DOCKER_COMPOSE ?= docker compose

up:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down -v

producer:
	python src/producer/main.py

consumer:
	python src/consumer/local_fallback_duckdb.py

query:
	python query_duckdb.py

register-connector:
	curl -X POST -H "Content-Type: application/json" --data @config/kafka_connect_snowflake.json http://localhost:8083/connectors

test-local:
	PYTHONPATH=. pytest tests/

clean:
	rm -rf crypto_trades.duckdb*
	rm -rf __pycache__
	rm -rf src/*/__pycache__
	rm -rf tests/__pycache__
