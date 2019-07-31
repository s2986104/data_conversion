#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/fpar"

python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
