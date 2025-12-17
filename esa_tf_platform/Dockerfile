FROM --platform=linux/amd64 continuumio/miniconda3:latest
ARG SEN2COR_RUN=Sen2Cor-02.12.03-Linux64.run
ARG TRACETOOL_ZIP=tracetool-1.2.4-distribution.zip
# By default the following glob doesn't match any file and the dependency is skipped
ARG EOPF_CPM=does-not-exixs[t]
ARG CONDA=conda

# `curl` and `unzip` are required to install tracetool
RUN apt remove python3 -y
RUN apt update && apt install -y \
    curl \
    file \
    gnupg \
    make \
    unzip \
    wget > 1.23.1 \
    zip \
    openssl

COPY $SEN2COR_RUN $TRACETOOL_ZIP /opt/

RUN conda create -n eopf python=3.11.9 -c conda-forge \
    && conda run -n eopf --no-capture-output pip install eopf==${EOPF_CPM} \
    && conda clean -afy
  # && conda run -n eopf --no-capture-output pip install ipython overrides bs4 \

RUN $CONDA install -c anaconda -c conda-forge openjdk=8 -y\
    && $CONDA clean -afy

RUN unzip /opt/$TRACETOOL_ZIP -d /opt
ENV PATH="${PATH}:/opt/"

RUN bash /opt/$SEN2COR_RUN --target /opt/sen2cor \
    && rm /opt/$SEN2COR_RUN

ENV PATH="${PATH}:/opt/sen2cor/bin"

COPY Makefile /opt/esa-tf-platform/
COPY environment.yml /opt/esa-tf-platform/

WORKDIR /opt/esa-tf-platform

RUN make conda-env-update ENVIRONMENT=base CONDA=conda && conda clean -afy

COPY . /opt/esa-tf-platform

RUN pip install .

RUN echo rm -rf /opt/esa-tf-platform/${TRACETOOL_ZIP} /opt/esa-tf-platform/${EOPF_CPM}.zip  /opt/esa-tf-platform/$SEN2COR_RUN
RUN rm -rf /opt/esa-tf-platform/${TRACETOOL_ZIP} /opt/esa-tf-platform/${EOPF_CPM}.zip  /opt/esa-tf-platform/$SEN2COR_RUN
RUN echo rm -rf /opt/${TRACETOOL_ZIP} /opt/${EOPF_CPM}.zip /opt/${EOPF_CPM}
RUN rm -rf /opt/${TRACETOOL_ZIP} /opt/${EOPF_CPM}.zip /opt/${EOPF_CPM}


RUN mkdir -p /working_dir

WORKDIR /working_dir
