#~/bin/sh
# tar bao sources
(cd .. &&\
    tar -czf bao.tar.gz *.py presto_query_plan queries tree_conv schema.sql requirements.txt &&\
    mv bao.tar.gz docker)

# get presto server and cli
# todo skip for now as repo is private: wget -N https://github.com/christophanneser/Bao-Presto-Integration/releases/download/v0.237-bao/presto-server.tar.gz

# build docker container
docker build . -f Dockerfile -t bao:latest
