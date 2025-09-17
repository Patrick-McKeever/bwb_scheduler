FROM debian:bookworm-slim
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ssh \
    && rm -rf /var/lib/apt/lists/*
COPY scanAndCopy.sh /usr/local/bin/scanAndCopy.sh
