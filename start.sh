#!/bin/bash

docker build --no-cache -t data_conversion .

docker run --rm -it -v $(PWD):/data_conversion -v /Volumes/USB/datasets:/mnt/collection/datasets data_conversion bash

# TODO: almost all of that below should be in Dockerfile (with cleanup and --no-cachedir)
# pip install -r requirements.txt
# pip install -e .

# TODO: apt needs the env var to tell it to use no or terminal config dialog
# apt-get update
# apt-get install gcc python3-dev

# pip install python-openstackclient python-swiftclient
# apt-get install unzip man-db
# curl https://rclone.org/install.sh | bash


# work as usual (just different top level path names)
