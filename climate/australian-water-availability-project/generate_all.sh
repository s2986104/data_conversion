#!/bin/sh

BASEDIR="/mnt/collection/datasets/climate/australian-water-availability-project"

python3 generate_layer_metadata.py --force ${BASEDIR}/layers
