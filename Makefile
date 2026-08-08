.PHONY: test lint typecheck

test:
	pytest

lint:
	ruff check src tests

typecheck:
	mypy src
