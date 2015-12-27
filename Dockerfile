FROM ubuntu:trusty
MAINTAINER Daniel Rodriguez

# Conda
RUN apt-get update && apt-get install -y -q curl bzip2
RUN curl http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
RUN /bin/bash /tmp/miniconda.sh -b -p /opt/conda
RUN rm /tmp/miniconda.sh
ENV PATH /opt/conda/bin:$PATH

# Libhdfs
RUN apt-get install -y -q git build-essential cmake libxml2 libxml2-dev uuid-dev protobuf-compiler libprotobuf-dev libgsasl7-dev libkrb5-dev libboost1.54-all-dev
RUN git clone https://github.com/PivotalRD/libhdfs3.git /opt/libhdfs3
ADD files/libhdfs-build.sh /tmp/libhdfs-build.sh
RUN bash /tmp/libhdfs-build.sh
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH

# libhdfs3.py
RUN /opt/conda/bin/conda install -y -q ipython cython
RUN /opt/conda/bin/pip install versioneer pywebhdfs click

VOLUME /libhdfs3.py
WORKDIR /libhdfs3.py
