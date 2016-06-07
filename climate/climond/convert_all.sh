!/bin/bash

for file in CLIMOND/MAP/*.zip 
do
    echo "convert $file"
    python convert.py $file bccvl
done
