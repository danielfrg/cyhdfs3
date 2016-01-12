FROM ubuntu:trusty
MAINTAINER Daniel Rodriguez

RUN apt-get update && apt-get install -y git build-essential curl bzip2

# conda
RUN curl http://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
RUN /bin/bash /tmp/miniconda.sh -b -p /opt/conda
RUN rm /tmp/miniconda.sh
ENV PATH /opt/conda/bin:$PATH
ENV PREFIX /opt/conda
# ENV LD_LIBRARY_PATH /opt/conda/lib:$LD_LIBRARY_PATH

# cyhdfs3
RUN conda install -y ipython cython pytest conda-build
RUN conda install -y -c https://conda.anaconda.org/blaze libhdfs3
RUN conda install -y -c https://conda.anaconda.org/mvn cyavro
RUN pip install versioneer twine pywebhdfs click
RUN conda create -y -n py2 python=2
RUN conda install -y -n py2 ipython cython pytest
RUN conda install -y -c https://conda.anaconda.org/blaze libhdfs3
RUN conda install -y -n py2 -c https://conda.anaconda.org/mvn cyavro
RUN /opt/conda/envs/py2/bin/pip install versioneer twine pywebhdfs click

VOLUME /cyavro
VOLUME /cyhdfs3
WORKDIR /cyhdfs3
