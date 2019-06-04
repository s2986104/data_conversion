#!/bin/sh


BASEDIR="/mnt/collection/datasets/environmental/global-marine"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers

# generate layer metadata
python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
