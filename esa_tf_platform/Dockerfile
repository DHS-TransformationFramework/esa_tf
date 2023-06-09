FROM continuumio/miniconda3
ARG SEN2COR_RUN=Sen2Cor-02.10.01-Linux64.run
ARG TRACETOOL_ZIP=tracetool-1.2.4-distribution.zip
# By default the following glob doesn't match any file and the dependency is skipped
ARG EOPF_CPM=does-not-exixs[t]

# `curl` and `unzip` are required to install tracetool
RUN apt update && apt install -y \
    curl \
    file \
    gnupg \
    make \
    unzip \
    zip

RUN conda install gdal hdf5 make virtualenv \
    # && conda install -c anaconda openjdk=8 \
    && conda clean -afy

# TODO check JAVA_HOME variable, if necessary set it
COPY $SEN2COR_RUN $TRACETOOL_ZIP $EOPF_CPM /opt/

RUN if [ -f /opt/$EOPF_CPM ]; then unzip /opt/$EOPF_CPM -d /opt \
    && touch /opt/PIPPO \
    && conda create -n eopf python=3.10.9 scipy=1.9.0 -c conda-forge\
    && conda init bash \
    && . ~/.bashrc \
    && conda activate eopf \
    && pip install /opt/${EOPF_CPM%%.zip}; fi

RUN unzip /opt/$TRACETOOL_ZIP -d /opt
ENV PATH="${PATH}:/opt/"

RUN conda install conda-libmamba-solver && conda config --set solver libmamba
RUN conda install make && conda install -c anaconda openjdk=8 && conda clean -afy

RUN bash /opt/$SEN2COR_RUN --target /opt/sen2cor \
    && rm /opt/$SEN2COR_RUN

ENV PATH="${PATH}:/opt/sen2cor/bin"

COPY Makefile /opt/esa-tf-platform/
COPY environment.yml /opt/esa-tf-platform/

WORKDIR /opt/esa-tf-platform

RUN make conda-env-update ENVIRONMENT=base && conda clean -afy

COPY . /opt/esa-tf-platform

RUN pip install .

RUN mkdir -p /processing

WORKDIR /processing
