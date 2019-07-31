#!/bin/sh

BASEDIR="/mnt/collection/datasets/climate/cru"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
