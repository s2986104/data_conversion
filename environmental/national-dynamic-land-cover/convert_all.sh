#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/national-dynamic-land-cover"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
