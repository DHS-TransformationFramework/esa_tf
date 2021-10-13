#!make

PORT = 8080

.PHONY: tests

COV_REPORT = html

.DEFAULT_GOAL := default

start:
	uvicorn --port $(PORT) esa_tf_restapi:app --reload

serve:
	uvicorn --port $(PORT) esa_tf_restapi:app

default: fix-code-style tests

fix-code-style:
	black .
	isort .

tests:
	python -m pytest -v --cov=. --cov-report=$(COV_REPORT) tests/

code-quality:
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
#	mypy --strict .

code-style:
	black --check .
	isort --check .
