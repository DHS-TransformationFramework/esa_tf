FROM continuumio/miniconda3

RUN conda install make

COPY Makefile /opt/esa-tf-platform/
COPY environment.yml /opt/esa-tf-platform/

WORKDIR /opt/esa-tf-platform

RUN make conda-env-update ENVIRONMENT=base

COPY . /opt/esa-tf-platform

RUN pip install .
RUN wget http://step.esa.int/thirdparties/sen2cor/2.9.0/Sen2Cor-02.09.00-Linux64.run
RUN bash Sen2Cor-02.09.00-Linux64.run