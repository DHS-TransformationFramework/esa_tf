FROM conda/miniconda3

RUN conda install -c conda-forge make mamba

COPY Makefile /opt/esa-tf-platform/
COPY environment.yml /opt/esa-tf-platform/

WORKDIR /opt/esa-tf-platform

RUN make mamba-env-update ENVIRONMENT=base

COPY . /opt/esa-tf-platform

RUN pip install .
