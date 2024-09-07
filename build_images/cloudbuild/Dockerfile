FROM python:3.10.8-slim-buster

COPY requirements.txt .
COPY .pre-commit-config.yaml .
COPY substitute.bash .

ARG CLOUD_SDK_VERSION=363.0.0
ENV CLOUD_SDK_VERSION=$CLOUD_SDK_VERSION
ENV PATH "$PATH:/opt/google-cloud-sdk/bin/"
RUN groupadd -r -g 1000 cloudsdk && \
    useradd -r -u 1000 -m -s /bin/bash -g cloudsdk cloudsdk
ARG INSTALL_COMPONENTS
RUN mkdir -p /usr/share/man/man1/
RUN apt-get update -qqy && apt-get install -qqy \
        curl \
        gcc \
        python3-dev \
        python3-pip \
        apt-transport-https \
        lsb-release \
        openssh-client \
        git \
        ruby-dev \
        libz-dev \
        libiconv-hook1  \
        libiconv-hook-dev  \
        build-essential \
        shellcheck  \
        gettext-base  \
        cmake \
        gnupg && \
    pip3 install -U crcmod && \
    export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb https://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get update && apt-get install -y google-cloud-sdk=${CLOUD_SDK_VERSION}-0 $INSTALL_COMPONENTS && \
    gcloud config set core/disable_usage_reporting true && \
    gcloud config set component_manager/disable_update_check true && \
    gcloud config set metrics/environment github_docker_image &&  \
    gcloud --version && \
    pip install -r requirements.txt
RUN apt-get install kubectl
RUN git config --system credential.'https://source.developers.google.com'.helper gcloud.sh

VOLUME ["/root/.config"]
