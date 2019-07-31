#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/nvis"

python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
