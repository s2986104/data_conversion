#!/bin/bash

# python3 convert_layers.py ./source /mnt/workdir/australia-5km/bccvl/layers
# python3 convert_layers.py ./source bccvl/layers

BASEDIR="/mnt/collection/datasets/climate/narclim"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
