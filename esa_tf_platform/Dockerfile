FROM --platform=linux/amd64 continuumio/miniconda3:latest
ARG SEN2COR_RUN=Sen2Cor-02.12.03-Linux64.run
# By default the following glob doesn't match any file and the dependency is skipped
ARG CONDA=conda

# `curl` and `unzip` are required to install
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

COPY $SEN2COR_RUN /opt/

COPY environment_eopf.yml .

RUN conda env create -f environment_eopf.yml && conda clean -afy

# RUN $CONDA install -c anaconda -c conda-forge openjdk=8 -y\
#     && $CONDA clean -afy

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

RUN echo rm -rf /opt/esa-tf-platform/$SEN2COR_RUN
RUN rm -rf /opt/esa-tf-platform/$SEN2COR_RUN


RUN mkdir -p /working_dir

WORKDIR /working_dir
