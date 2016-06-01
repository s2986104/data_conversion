!/bin/bash

for file in TASCLIM/MAP/*.zip 
do
    echo "convert $file"
    python convert.py $file bccvl
done
