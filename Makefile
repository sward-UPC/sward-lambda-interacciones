.PHONY: test lint

test:
	pip install -r requirements-dev.txt
	pytest tests/ -v

lint:
	ruff check . && ruff format --check .
