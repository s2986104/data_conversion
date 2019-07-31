#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/global-marine"

python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
