#!/bin/sh

BASEDIR="/mnt/collection/datasets/climate/anuclim"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
