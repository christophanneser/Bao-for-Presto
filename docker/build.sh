#!/bin/sh

# archive bao sources which will be copied into the container
(cd .. &&\
    tar -czf bao.tar.gz *.py presto_query_plan queries tree_conv schema.sql requirements.txt &&\
    mv bao.tar.gz docker)

# get presto server release
wget -N https://github.com/christophanneser/Bao-Presto-Integration/releases/download/v0.237-bao/presto-server.tar.gz

# build bao-presto container
docker build . -f Dockerfile -t bao:latest
