FROM continuumio/miniconda3

RUN conda install make

COPY Makefile /opt/esa-tf-restapi/
COPY environment.yml /opt/esa-tf-restapi/

WORKDIR /opt/esa-tf-restapi

RUN make conda-env-update ENVIRONMENT=base && conda clean -afy

COPY . /opt/esa-tf-restapi

RUN pip install .