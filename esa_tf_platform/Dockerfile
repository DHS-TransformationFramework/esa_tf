FROM continuumio/miniconda3
ARG SEN2COR_RUN=Sen2Cor-02.09.00-Linux64.run

RUN apt update && apt install -y \
    file \
    make

COPY $SEN2COR_RUN /opt

RUN bash /opt/$SEN2COR_RUN --target /opt/sen2cor \
    && rm /opt/$SEN2COR_RUN

RUN export PATH=$PATH:/opt/sen2cor/bin

COPY Makefile /opt/esa-tf-platform/
COPY environment.yml /opt/esa-tf-platform/

WORKDIR /opt/esa-tf-platform

RUN make conda-env-update ENVIRONMENT=base

COPY . /opt/esa-tf-platform

RUN pip install .
