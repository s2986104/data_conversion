#!/bin/sh

BASEDIR="/mnt/collection/datasets/climate/narclim"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
