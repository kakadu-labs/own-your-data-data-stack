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

bootstrap_k8s:
	k3d cluster create --config infrastructure/k3d/config.yaml
	k3d kubeconfig merge kl-own-your-data-k8s --output infrastructure/secrets/kubeconfig.yaml

destroy_k8s:
	k3d cluster delete kl-own-your-data-k8s

forward-all-ports:
	ssh -L 5432:localhost:5432 -L 9090:localhost:8080  -L 9000:localhost:9000 -L 9001:localhost:9001 root@49.13.117.87

bootstrap-infra-docker:
	docker compose up -d
