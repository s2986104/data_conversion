#!/bin/sh

BASEDIR="/mnt/collection/datasets/climate/australian-water-availability-project"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
