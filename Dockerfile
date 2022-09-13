FROM python:3.9-slim

LABEL maintainer "RnDFlow <mail@rndflow.com>"

#RUN groupadd -r rndflow && useradd -r -s /sbin/nologin -g rndflow rndflow

RUN apt-get update && apt-get upgrade -y && apt-get install -y moreutils procps time && rm -r /var/lib/apt/lists /var/cache/apt/archives

COPY requirements.txt requirements.txt
COPY RELEASE-VERSION RELEASE-VERSION

RUN pip install -r requirements.txt
COPY . /distr
RUN pip install /distr && rm -rf /distr

##RUN mkdir /work && chown -R rndflow:rndflow /work #'workingDir': '/work' # #https://github.com/rndflow/rndflow-executor-k8s/blob/master/app/executor.py#L80 # not work

RUN chmod -v a-s /usr/bin/chsh /bin/su /sbin/unix_chkpwd /usr/bin/chage /usr/bin/passwd /bin/umount /bin/mount /usr/bin/chfn /usr/bin/wall /usr/bin/gpasswd /usr/bin/newgrp /usr/bin/expiry

WORKDIR /work

#USER rndflow
