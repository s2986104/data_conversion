#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/aus-topography"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
