#!/bin/sh

BASEDIR="/mnt/collection/datasets/climate/global-pet-and-aridity"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
