.PHONY: setup init-local validate show-paths ingest-steam-app-catalog stage-steam-app-catalog test lint dbt-debug

setup:
	python -m pip install -e ".[dev]"

init-local:
	python -m game_market_analytics.cli init-local

validate:
	python -m game_market_analytics.cli validate-project

show-paths:
	python -m game_market_analytics.cli show-paths

ingest-steam-app-catalog:
	python -m game_market_analytics.cli ingest-steam-app-catalog

stage-steam-app-catalog:
	python -m game_market_analytics.cli stage-steam-app-catalog

test:
	python -m pytest

lint:
	python -m ruff check src tests

dbt-debug:
	cd dbt && dbt debug --profiles-dir .
