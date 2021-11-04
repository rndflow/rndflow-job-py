FROM python:3.9-slim
RUN apt-get update && apt-get install -y moreutils procps

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . /distr
RUN pip install /distr && rm -rf /distr

WORKDIR /rndflow
