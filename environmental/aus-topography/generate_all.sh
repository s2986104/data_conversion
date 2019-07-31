#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/aus-topography"

python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
