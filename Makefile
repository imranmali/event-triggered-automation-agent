.PHONY: install lint test test-all run demo-up demo-down

install:
	pip install -e ".[dev]"

lint:
	ruff check src tests scripts && ruff format --check src tests scripts && mypy

test:
	pytest -q

# Requires Postgres and Redis (e.g. `docker compose up -d postgres redis`).
test-all:
	ETA_TEST_DATABASE_URL=postgresql+asyncpg://eta:eta@localhost:5432/eta_test \
	ETA_TEST_REDIS_URL=redis://localhost:6379/15 pytest -q

run:
	eta serve --worker --consumers --migrate

demo-up:
	docker compose up --build -d

demo-down:
	docker compose down -v
