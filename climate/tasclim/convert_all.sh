#!/bin/sh

BASEDIR="/mnt/collection/datasets/climate/tasclim"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
