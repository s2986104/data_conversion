#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/national-dynamic-land-cover"

python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
