FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
   build-essential \
   libseccomp-dev \
   libglib2.0-dev \
   pkg-config \
   squashfs-tools \
   cryptsetup \
   runc \
   wget \
   docker.io

ENV GO_VERSION=1.18.1 OS=linux ARCH=amd64
RUN wget https://dl.google.com/go/go$GO_VERSION.$OS-$ARCH.tar.gz && \
    tar -C /usr/local -xzvf go$GO_VERSION.$OS-$ARCH.tar.gz && \
    rm go$GO_VERSION.$OS-$ARCH.tar.gz

ENV PATH="/usr/local/go/bin:${PATH}"

ENV VERSION=3.10.0

RUN wget https://github.com/sylabs/singularity/releases/download/v${VERSION}/singularity-ce-${VERSION}.tar.gz && \
    tar -xzf singularity-ce-${VERSION}.tar.gz && \
    cd singularity-ce-${VERSION} && \
    ./mconfig && \
    make -C builddir && \
    make -C builddir install


RUN singularity version

RUN wget https://dl.google.com/go/go1.24.4.linux-amd64.tar.gz
RUN mkdir -p /usr/local/go1.24.4
RUN tar -C /usr/local/go1.24.4 --strip-components=1 -xzf go1.24.4.linux-amd64.tar.gz \
    && rm go1.24.4.linux-amd64.tar.gz
ENV PATH="/usr/local/go1.24.4/bin:${PATH}"
RUN go version

COPY . /sched-src
WORKDIR /sched-src
RUN go build 
RUN mv go-scheduler /usr/bin/bwbSched
#CMD ["bwbSched", "--help"]