.PHONY: setup test lint dbt-debug

setup:
	python -m pip install -e ".[dev]"

test:
	python -m pytest

lint:
	python -m ruff check src tests

dbt-debug:
	cd dbt && dbt debug
