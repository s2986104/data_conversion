#!/bin/bash

# python3 convert_layers.py ./source /mnt/workdir/australia-5km/bccvl/layers
# python3 convert_layers.py ./source bccvl/layers

BASEDIR="/mnt/collection/datasets/climate/global-pet-and-aridity"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers

# generate layer metadata
python3 generate_layer_metadata.py --force ${BASEDIR}/layers 