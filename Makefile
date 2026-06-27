.PHONY: setup run test lint clean dev

setup:
	python -m venv .venv
	.venv/bin/pip install -e ".[dev]"

run:
	mkdir -p output
	.venv/bin/dagster asset materialize --select '*' -m src.definitions

test:
	.venv/bin/pytest tests/ -v

dev:
	mkdir -p output
	.venv/bin/dagster dev -m src.definitions

clean:
	rm -rf output/helix.duckdb
