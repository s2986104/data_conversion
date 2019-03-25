#!/bin/bash


BASEDIR="/mnt/collection/datasets/environmental/national_soil_grids"

python3 convert_layers.py ${BASEDIR}/source ${BASEDIR}/layers
