#!/bin/sh

BASEDIR="/mnt/collection/datasets/climate/worldclim"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
