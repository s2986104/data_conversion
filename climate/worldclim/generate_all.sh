#!/bin/bash

BASEDIR="/mnt/collection/datasets/climate/worldclim"

python3 generate_layer_metadata.py --force ${BASEDIR}/layers 
