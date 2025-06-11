format:
	uv run ruff check --select I --fix
	uv run ruff format

lint:
	uv run ruff check

test:
	uv run pytest

test-unit:
	uv run pytest -v -s tests/unit

test-integration:
	uv run pytest -v -s tests/integration


airflow-bootstrap:
	uv run airflow standalone
