#!/bin/sh

BASEDIR="/mnt/collection/datasets/climate/climond"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
