#!make

PORT := 8080
ENVIRONMENT := ESA_TF_RESTAPI
IMAGE_NAME := collaborativedhs/esa_tf_restapi
COV_REPORT := html
CONDA := conda

.DEFAULT_GOAL := default
DOCKERFLAGS ?= --platform linux/amd64

start:
	SCHEDULER=$(SCHEDULER) uvicorn --proxy-headers --port $(PORT) esa_tf_restapi:app --reload

default: fix-code-style test

fix-code-style:
	black .
	isort .
	mdformat .

test:
	python -m pytest -v --cov=. --cov-report=$(COV_REPORT) tests/

code-quality:
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
#	mypy --strict .

code-style:
	black --check .
	isort --check .
	mdformat --check .

# deploy

conda-env-create:
	${CONDA} env create -n $(ENVIRONMENT) -f environment.yml

conda-env-update:
	${CONDA} env update -n $(ENVIRONMENT) -f environment.yml

image:
	docker build -t $(IMAGE_NAME) $(DOCKERFLAGS) .

# run

serve:
# 	set WEB_CONCURRENCY envvar to control number of workers
	SCHEDULER=$(SCHEDULER) gunicorn -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$(PORT) esa_tf_restapi:app