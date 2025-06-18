# hash:sha256:ef64ff17fc6ca0c678f82140d5b19c8259e32919a509a4d099642e97efe36d36
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/mambaforge3:23.1.0-4-python3.10.12-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive

ARG GIT_ASKPASS
ARG GIT_ACCESS_TOKEN
COPY git-askpass /

RUN pip install --no-cache-dir \
    aind_data_access_api[docDB] \
    aind-dynamic-foraging-models \
    aind-analysis-arch-result-access

COPY postInstall /
RUN /postInstall
