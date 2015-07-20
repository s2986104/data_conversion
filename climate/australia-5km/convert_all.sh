#!/bin/bash

for file in source/*.zip ; do
    echo "convert $file"
    python convert.py $file bccvl
done
