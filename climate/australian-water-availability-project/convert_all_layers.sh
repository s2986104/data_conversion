#!/bin/bash

for file in source/*.zip ; do
    echo "convert $file"
    python convert_layers.py $file bccvl/layers
done
