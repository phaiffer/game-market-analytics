.PHONY: setup init-local validate show-paths ingest-steam-app-catalog ingest-steam-reviews stage-steam-app-catalog stage-steam-reviews test lint dbt-init-profile dbt-debug dbt-run dbt-test dbt-build

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

ingest-steam-reviews:
	python -m game_market_analytics.cli ingest-steam-reviews

stage-steam-app-catalog:
	python -m game_market_analytics.cli stage-steam-app-catalog

stage-steam-reviews:
	python -m game_market_analytics.cli stage-steam-reviews

test:
	python -m pytest

lint:
	python -m ruff check src tests

dbt-init-profile:
	powershell -NoProfile -Command "if (-not (Test-Path 'dbt/profiles.yml')) { Copy-Item 'dbt/profiles.example.yml' 'dbt/profiles.yml' }"

dbt-debug:
	dbt debug --project-dir dbt --profiles-dir dbt

dbt-run:
	dbt run --project-dir dbt --profiles-dir dbt

dbt-test:
	dbt test --project-dir dbt --profiles-dir dbt

dbt-build:
	dbt build --project-dir dbt --profiles-dir dbt
