#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/nvis"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
