FROM debian:bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt install -y minimap2 samtools \
    && rm -rf /var/lib/apt/lists/*
COPY runMiniMap2.sh /usr/local/bin/runMiniMap2.sh
