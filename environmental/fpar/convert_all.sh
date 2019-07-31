#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/fpar"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
