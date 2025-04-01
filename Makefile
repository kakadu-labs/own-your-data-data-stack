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

generate_csv_stream:
	LOG_LEVEL=info uv run python generate_csv_stream.py

consume_csv_stream:
	LOG_LEVEL=info uv run python read_csv_as_stream.py
