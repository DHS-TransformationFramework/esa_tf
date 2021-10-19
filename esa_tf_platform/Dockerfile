FROM continuumio/miniconda3

RUN conda install make

COPY Makefile /opt/esa-tf-platform/
COPY environment.yml /opt/esa-tf-platform/

WORKDIR /opt/esa-tf-platform

RUN make conda-env-update ENVIRONMENT=base

COPY . /opt/esa-tf-platform

RUN pip install .
