#!/bin/sh

BASEDIR="/mnt/collection/datasets/climate/australia"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
