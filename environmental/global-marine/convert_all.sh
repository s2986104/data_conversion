#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/global-marine"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
