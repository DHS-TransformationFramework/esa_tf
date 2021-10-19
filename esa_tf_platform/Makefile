#!make

ENVIRONMENT := ESA_TF_PLATFORM
DASKFLAGS :=
IMAGE_NAME := esa_tf_worker
COV_REPORT := html

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

dask-worker:
	dask-worker $(DASKFLAGS)
