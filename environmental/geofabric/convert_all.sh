#!/bin/bash

# This is for collections for geofabric data, which is handled differently from other datasets.
# Run this to generate the data layers for Geofabric data 

BASEDIR="/mnt/collection/datasets/environmental/geofabric

python3 convert_layers.py ${BASEDIR}/source/NationalCatchmentBoundariesRaster1.tif ${BASEDIR}/layers
python3 convert_layers.py ${BASEDIR}/source/DEMDerivedStreamsRaster1.tif ${BASEDIR}/layers
