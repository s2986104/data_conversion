#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/vast"

python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
