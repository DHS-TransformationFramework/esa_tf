FROM condaforge/mambaforge

RUN conda install conda-libmamba-solver && conda config --set solver libmamba
RUN conda install make && conda clean -afy

COPY Makefile /opt/esa-tf-restapi/
COPY environment.yml /opt/esa-tf-restapi/

WORKDIR /opt/esa-tf-restapi

RUN make conda-env-update ENVIRONMENT=base CONDA=mamba && mamba clean -afy

COPY . /opt/esa-tf-restapi

RUN pip install .
