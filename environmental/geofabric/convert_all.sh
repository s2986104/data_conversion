#!/bin/sh

BASEDIR="/mnt/collection/datasets/environmental/geofabric"

python3 convert_layers.py ${BASEDIR}/source/NationalCatchmentBoundariesRaster1.tif ${BASEDIR}/layers
python3 convert_layers.py ${BASEDIR}/source/DEMDerivedStreamsRaster1.tif ${BASEDIR}/layers
