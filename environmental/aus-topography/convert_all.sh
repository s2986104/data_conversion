#!/bin/bash

# This is for Australian Topography collection
# Run this to for multi-resolution-valley-bottom-flatness
# and multi-resolution-ridge-top-flatness datasets

BASEDIR="/mnt/collection/datasets/environmental/aus-topography"
MRVBF_DIR="/mnt/collection/datasets/environmental/multi-resolution-valley-bottom-flatness"
MRRTF_DIR="/mnt/collection/datasets/environmental/multi-resolution-ridge-top-flatness"

python3 convert_layers.py ${MRRTF_DIR}/source ${BASEDIR}/layers
python3 convert_layers.py ${MRVBF_DIR}/source ${BASEDIR}/layers
