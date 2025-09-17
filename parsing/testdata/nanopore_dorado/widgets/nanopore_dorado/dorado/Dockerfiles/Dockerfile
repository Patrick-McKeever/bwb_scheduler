FROM debian:bookworm-slim
ARG TARGETPLATFORM=linux/amd64
ARG VERSION=0.5.3
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y curl samtools && apt-get clean -y && apt-get autoclean -y && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*   
RUN case ${TARGETPLATFORM} in \
    'linux/amd64') \
      curl -L https://cdn.oxfordnanoportal.com/software/analysis/dorado-${VERSION}-linux-x64.tar.gz | tar xz -C /usr/local/ --strip-components=1 ;; \
    'linux/arm64') \
      curl -L https://cdn.oxfordnanoportal.com/software/analysis/dorado-${VERSION}-linux-arm64.tar.gz | tar xz -C /usr/local/ --strip-components=1 ;; \
    *) \
      echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac
COPY runDorado.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/runDorado.sh