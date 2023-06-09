#!make

ENVIRONMENT := ESA_TF_PLATFORM
DASKFLAGS :=
IMAGE_NAME := collaborativedhs/esa_tf_worker
SEN2COR_RUN := Sen2Cor-02.10.01-Linux64.run
SEN2COR_URL := http://step.esa.int/thirdparties/sen2cor/2.10.0/$(SEN2COR_RUN)
COV_REPORT := html
TRACETOOL_ZIP := tracetool-1.2.4-distribution.zip
TRACETOOL_URL := https://repository.gael-systems.com/repository/public/fr/gael/datac/tracetool/1.2.4/$(TRACETOOL_ZIP)
EOPF_CPM := eopf-cpm-1.2.2.zip

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

$(TRACETOOL_ZIP):
	curl -LO $(TRACETOOL_URL)

setup: $(SEN2COR_RUN) $(TRACETOOL_ZIP)

image: $(SEN2COR_RUN) $(TRACETOOL_ZIP) $(EOPF_CPM)
	docker build -t $(IMAGE_NAME) --build-arg SEN2COR_RUN=$(SEN2COR_RUN) --build-arg TRACETOOL_ZIP=$(TRACETOOL_ZIP) --build-arg EOPF_CPM=$(EOPF_CPM) .

# HACK: the EOPF_CMP must be a glob matching no file
image-no-eopf: $(SEN2COR_RUN) $(TRACETOOL_ZIP)
	docker build -t $(IMAGE_NAME) --build-arg SEN2COR_RUN=$(SEN2COR_RUN) --build-arg TRACETOOL_ZIP=$(TRACETOOL_ZIP) .

shell:
	docker-compose -f ../esa_tf/docker-compose.yml run --rm $(IMAGE_NAME) bash

# run

dask-worker:
	dask-worker $(DASKFLAGS)
