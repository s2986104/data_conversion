#!/bin/sh

BASEDIR="/mnt/collection/datasets/climate/anuclim"

python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
