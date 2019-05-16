#!/bin/bash


BASEDIR="/mnt/collection/datasets/environmental/national_soil_grids"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers

# generate layer metadata
python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
