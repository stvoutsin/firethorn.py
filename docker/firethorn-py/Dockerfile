FROM ubuntu:16.04
MAINTAINER Stelios Voutsinas <docker-stv@roe.ac.uk>

ENV LANG C.UTF-8
RUN apt-get update && apt-get install -y python python-dev python3.5 python3.5-dev python-pip virtualenv libssl-dev libpq-dev git build-essential libfontconfig1 libfontconfig1-dev python3-pip libcurl4-openssl-dev unixodbc-bin unixodbc-dev nano curl
RUN pip install setuptools pip --upgrade --force-reinstall
RUN pip install virtualenv
RUN pip3 install --upgrade pip
RUN git clone https://github.com/stvoutsin/firethorn.py.git /home/firethorn.py
WORKDIR /home

RUN  pip3 install firethorn.py/
CMD  pip3 install firethorn.py/

