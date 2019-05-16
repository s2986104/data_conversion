#!/bin/bash

# This is for Australian Topography collection
# Run this to for multi-resolution-valley-bottom-flatness
# and multi-resolution-ridge-top-flatness datasets


BASEDIR="/mnt/collection/datasets/environmental/national-dynamic-land-cover"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers

# generate layer metadata
python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
