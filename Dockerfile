FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    software-properties-common

RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.10 python3.10-dev python3.10-distutils

RUN apt-get install -y openjdk-11-jdk

RUN apt-get update && \
    apt-get install -y python3-pip python3-dev command-not-found && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

COPY requirements.txt .

RUN pip install -r requirements.txt

WORKDIR /local_workspace

CMD ["bash"]