#!/bin/bash

function set_build_status() {
    echo $1 > build_status;
    sudo mv build_status /mnt/nvm2/unprivileged/;
    sudo chown -R unprivileged:unprivileged /mnt/nvm2/unprivileged/;
}

sudo echo "got root access";

set_build_status 0;

# build all the docker images from scratch
./build.sh

# export the images
echo "export all images"
docker save presto:java-coordinator presto:java-worker | pigz -p 16 > presto_coord_and_worker.tar.gz;
# docker save  | pigz -p 16 > presto_worker.tar.gz;
sudo mv presto_coord_and_worker.tar.gz /mnt/nvm2/unprivileged/;
sudo chown -R unprivileged:unprivileged /mnt/nvm2/unprivileged/;

set_build_status 1;

# tar -cvf configs.tar .configs
# sudo mv configs.tar /mnt/nvm2/unprivileged/

# docker save presto:java-coordinator | pigz -p 16 > presto_coordinator.tar.gz;
# sudo mv presto_coordinator.tar.gz /mnt/nvm2/unprivileged/;

