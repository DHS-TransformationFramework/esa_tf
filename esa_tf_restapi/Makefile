#!make

PORT := 8080
ENVIRONMENT := ESA_TF_RESTAPI
IMAGE_NAME := ghcr.io/bopen/esa_tf_restapi
COV_REPORT := html

.DEFAULT_GOAL := default

start:
	SCHEDULER=$(SCHEDULER) uvicorn --port $(PORT) esa_tf_restapi:app --reload

default: fix-code-style test

fix-code-style:
	black .
	isort .

test:
	python -m pytest -v --cov=. --cov-report=$(COV_REPORT) tests/

code-quality:
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
#	mypy --strict .

code-style:
	black --check .
	isort --check .

# deploy

conda-env-create:
	conda env create -n $(ENVIRONMENT) -f environment.yml

conda-env-update:
	conda env update -n $(ENVIRONMENT) -f environment.yml

image:
	docker build -t $(IMAGE_NAME) .

# run

serve:
# 	set WEB_CONCURRENCY envvar to control number of workers
	SCHEDULER=$(SCHEDULER) gunicorn -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$(PORT) esa_tf_restapi:app