FROM continuumio/miniconda3
ARG TRACETOOL_ZIP=tracetool-1.2.4-distribution.zip

# `curl` and `unzip` are required to install tracetool
RUN apt update && apt install -y \
    unzip \
    gnupg \
    curl

# TODO check JAVA_HOME variable, if necessary set it
RUN curl -LO https://repository.gael-systems.com/repository/public/fr/gael/datac/tracetool/1.2.4/$TRACETOOL_ZIP
RUN unzip $TRACETOOL_ZIP -d /opt
ENV PATH="${PATH}:/opt/"

RUN conda install make && conda install -c anaconda openjdk=8 && conda clean -afy

COPY Makefile /opt/esa-tf-restapi/
COPY environment.yml /opt/esa-tf-restapi/

WORKDIR /opt/esa-tf-restapi

RUN make conda-env-update ENVIRONMENT=base && conda clean -afy

COPY . /opt/esa-tf-restapi

RUN pip install .
