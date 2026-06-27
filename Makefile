.PHONY: setup run test clean dev

AIRFLOW_HOME ?= $(CURDIR)/.airflow_home
export AIRFLOW_HOME

setup:
	python -m venv .venv
	.venv/bin/pip install -e ".[dev]"
	AIRFLOW_HOME=$(AIRFLOW_HOME) .venv/bin/airflow db migrate
	AIRFLOW_HOME=$(AIRFLOW_HOME) .venv/bin/airflow connections add 'helix_duckdb' \
		--conn-type 'generic' \
		--conn-extra '{"db_path": "output/helix.duckdb"}' 2>/dev/null || true

run:
	mkdir -p output
	AIRFLOW_HOME=$(AIRFLOW_HOME) HELIX_DATA_DIR=data HELIX_DB_PATH=output/helix.duckdb \
		.venv/bin/airflow dags test helix_pipeline

test:
	.venv/bin/pytest tests/ -v

dev:
	mkdir -p output
	AIRFLOW_HOME=$(AIRFLOW_HOME) HELIX_DATA_DIR=data HELIX_DB_PATH=output/helix.duckdb \
		.venv/bin/airflow standalone

clean:
	rm -rf output/helix.duckdb .airflow_home/
