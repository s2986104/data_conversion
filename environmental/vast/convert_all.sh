#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/vast"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
