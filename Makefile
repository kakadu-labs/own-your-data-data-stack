format:
	uv run ruff check --select I --fix
	uv run ruff format

lint:
	uv run ruff check

run_data_generator:
	uv run python data_generator.py

init_state:
	rm -r bronze.delta
	rm -r ckpts
