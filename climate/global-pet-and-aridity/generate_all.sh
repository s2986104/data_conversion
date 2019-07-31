#!/bin/sh

BASEDIR="/mnt/collection/datasets/climate/global-pet-and-aridity"

python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
