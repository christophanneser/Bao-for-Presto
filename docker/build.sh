#!/bin/sh

# archive bao sources which will be copied into the container (without pytorch)
(
    cd .. &&\
    cp requirements.txt minimal_requirements.txt && sed -i '/torch/d' minimal_requirements.txt &&\
    tar -czf bao.tar.gz *.py presto_query_plan queries tree_conv schema.sql minimal_requirements.txt &&\
    mv bao.tar.gz docker && cp docker/bao.tar.gz Bao-Presto-Integration/Docker/bao.tar.gz

)

# build presto server from submodule
(
    cd ../Bao-Presto-Integration &&\
    ./package-presto-for-docker.sh &&\
    cd Docker &&\
    ./build.sh
)


