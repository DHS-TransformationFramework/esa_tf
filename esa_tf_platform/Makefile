#!make

ENVIRONMENT := ESA_TF_PLATFORM
DASKFLAGS :=
IMAGE_NAME := ghcr.io/bopen/esa_tf_worker
SEN2COR_RUN := Sen2Cor-02.09.00-Linux64.run
SEN2COR_URL := http://step.esa.int/thirdparties/sen2cor/2.9.0/$(SEN2COR_RUN)
COV_REPORT := html

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
	conda env create -n $(ENVIRONMENT) -f environment.yml

conda-env-update:
	conda env update -n $(ENVIRONMENT) -f environment.yml

$(SEN2COR_RUN):
	curl -LO $(SEN2COR_URL)

setup: $(SEN2COR_RUN)

image: $(SEN2COR_RUN)
	docker build -t $(IMAGE_NAME) --build-arg SEN2COR_RUN=$(SEN2COR_RUN) .

shell:
	docker-compose -f ../esa_tf/docker-compose.yml run --rm $(IMAGE_NAME) bash

# run

dask-worker:
	dask-worker $(DASKFLAGS)
