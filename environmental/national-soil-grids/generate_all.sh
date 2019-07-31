#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/national_soil_grids"

python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
