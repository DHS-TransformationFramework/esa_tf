FROM condaforge/mambaforge
ARG SEN2COR_RUN=Sen2Cor-02.10.01-Linux64.run
ARG TRACETOOL_ZIP=tracetool-1.2.4-distribution.zip
# By default the following glob doesn't match any file and the dependency is skipped
ARG EOPF_CPM=does-not-exixs[t]

# `curl` and `unzip` are required to install tracetool
RUN apt update && apt install -y \
    curl \
    file \
    gnupg \
    unzip \
    zip

RUN mamba install gdal hdf5 make virtualenv -y\
    && mamba install -c anaconda -c conda-forge cxx-compiler openjdk=8 -y\
    && mamba clean -afy

# TODO check JAVA_HOME variable, if necessary set it
COPY $SEN2COR_RUN $TRACETOOL_ZIP ${EOPF_CPM}.zip /opt/

RUN unzip /opt/${EOPF_CPM}.zip -d /opt \
    && mamba create -n eopf python=3.10.9 scipy=1.9.0 cfgrib=0.9.10.1 -c conda-forge \
    && mamba run -n eopf pip install /opt/$EOPF_CPM\
    && mamba clean -afy

RUN unzip /opt/$TRACETOOL_ZIP -d /opt
ENV PATH="${PATH}:/opt/"

RUN bash /opt/$SEN2COR_RUN --target /opt/sen2cor \
    && rm /opt/$SEN2COR_RUN

ENV PATH="${PATH}:/opt/sen2cor/bin"

COPY Makefile /opt/esa-tf-platform/
COPY environment.yml /opt/esa-tf-platform/

WORKDIR /opt/esa-tf-platform

RUN make conda-env-update ENVIRONMENT=base CONDA=mamba && mamba clean -afy

COPY . /opt/esa-tf-platform

RUN pip install .

RUN mkdir -p /working_dir

WORKDIR /working_dir
