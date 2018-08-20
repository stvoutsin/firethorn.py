FROM ubuntu:16.04
MAINTAINER Stelios Voutsinas <docker-stv@roe.ac.uk>

ENV LANG C.UTF-8
RUN apt-get update && apt-get install -y  python3 python3-pip nano git
RUN pip3 install --upgrade pip
RUN git clone https://github.com/stvoutsin/firethorn.py.git /home/firethorn.py
RUN git clone https://github.com/stvoutsin/firethorn-utils.git /home/firethorn-utils

WORKDIR /home

RUN  pip3 install firethorn.py/
CMD  pip3 install firethorn.py/

RUN  pip3 install firethorn-utils/
CMD  pip3 install firethorn-utils/
