#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/marspec"

python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
