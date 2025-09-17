ARG ALPINE_VERSION=3.15

FROM alpine:${ALPINE_VERSION} AS alpine-build-samtools

ARG SAMTOOLS_VERSION=1.13

RUN apk update && apk add build-base wget zlib-dev tar bzip2-dev xz-dev \
    curl-dev ncurses-dev ncurses-static curl-static zlib-static bzip2-static \
    nghttp2-static openssl-libs-static brotli-static

WORKDIR /build
RUN wget https://github.com/samtools/samtools/releases/download/${SAMTOOLS_VERSION}/samtools-${SAMTOOLS_VERSION}.tar.bz2
RUN tar -xf samtools-${SAMTOOLS_VERSION}.tar.bz2
WORKDIR /build/samtools-${SAMTOOLS_VERSION}
RUN ./configure && \
    make -j 4
RUN strip samtools

FROM alpine:${ALPINE_VERSION}
ARG SAMTOOLS_VERSION=1.13
RUN apk add --no-cache bash perl libbz2 zlib libcurl xz-libs &&  rm -rf /var/cache/apk/*
WORKDIR / 
COPY --from=alpine-build-samtools /build/samtools-${SAMTOOLS_VERSION}/samtools /usr/local/bin/samtools
ADD filterSam.pl /usr/local/bin/filterSam.pl
ADD filterEnds.pl /usr/local/bin/filterEnds.pl
ADD runBFF.sh /usr/local/bin/runBFF.sh
