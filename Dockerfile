FROM ubuntu:trusty
MAINTAINER Daniel Rodriguez

RUN apt-get update && apt-get install -y -q curl bzip2

# Cloudera repositories
RUN curl -s http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/archive.key | apt-key add -
RUN echo 'deb [arch=amd64] http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh trusty-cdh5 contrib' > /etc/apt/sources.list.d/cloudera.list
RUN echo 'deb-src http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh trusty-cdh5 contrib' >> /etc/apt/sources.list.d/cloudera.list
ADD files/cloudera.pref /etc/apt/preferences.d/cloudera.pref

# Install CDH5 in a single node: Pseudo Distributed
ADD files/install.sh /tmp/install.sh
RUN bash /tmp/install.sh

# Conda
RUN curl http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
RUN /bin/bash /tmp/miniconda.sh -b -p /opt/conda
RUN rm /tmp/miniconda.sh
RUN /opt/conda/bin/conda install -y -q conda-build
ENV PATH /opt/conda/bin:$PATH

# Libhdfs
RUN apt-get install -y -q git build-essential cmake libxml2 libxml2-dev uuid-dev protobuf-compiler libprotobuf-dev libgsasl7-dev libkrb5-dev libboost1.54-all-dev
RUN git clone https://github.com/PivotalRD/libhdfs3.git /opt/libhdfs3
ADD files/libhdfs-build.sh /tmp/libhdfs-build.sh
RUN bash /tmp/libhdfs-build.sh

# libhdfs3.py
RUN /opt/conda/bin/conda install -y -q cython

EXPOSE 8020
EXPOSE 50070
VOLUME /libhdfs3py
WORKDIR /libhdfs3py

ADD start.sh /tmp/start.sh
CMD ["bash", "/tmp/start.sh"]
