#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/marspec"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
