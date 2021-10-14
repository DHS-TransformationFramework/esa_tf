FROM conda/miniconda3

RUN conda install -c conda-forge make mamba

COPY Makefile /opt/esa-tf-restapi/
COPY environment.yml /opt/esa-tf-restapi/

WORKDIR /opt/esa-tf-restapi

RUN make mamba-env-update ENVIRONMENT=base

COPY . /opt/esa-tf-restapi

RUN pip install .