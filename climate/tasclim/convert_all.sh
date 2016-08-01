!/bin/bash

for file in source/MAP/*.zip 
do
    echo "convert $file"
    python convert.py $file bccvl
done
