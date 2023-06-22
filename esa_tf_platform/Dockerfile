FROM continuumio/miniconda3
ARG SEN2COR_RUN=Sen2Cor-02.10.01-Linux64.run
ARG TRACETOOL_ZIP=tracetool-1.2.4-distribution.zip

# `curl` and `unzip` are required to install tracetool
RUN apt update && apt install -y \
    file \
    make \
    unzip \
    gnupg \
    curl \
    zip

# TODO check JAVA_HOME variable, if necessary set it
RUN curl -LO https://repository.gael-systems.com/repository/public/fr/gael/datac/tracetool/1.2.4/$TRACETOOL_ZIP
RUN unzip $TRACETOOL_ZIP -d /opt
ENV PATH="${PATH}:/opt/"

RUN conda install make && conda install -c anaconda -c conda-forge cxx-compiler openjdk=8 && conda clean -afy

COPY $SEN2COR_RUN /opt

RUN bash /opt/$SEN2COR_RUN --target /opt/sen2cor \
    && rm /opt/$SEN2COR_RUN

ENV PATH="${PATH}:/opt/sen2cor/bin"

COPY Makefile /opt/esa-tf-platform/
COPY environment.yml /opt/esa-tf-platform/

WORKDIR /opt/esa-tf-platform

RUN make conda-env-update ENVIRONMENT=base && conda clean -afy

COPY . /opt/esa-tf-platform

RUN pip install .
