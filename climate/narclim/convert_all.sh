!/bin/bash

for file in NaRClim/NarCLIM_1km/*.zip 
do
    echo "convert $file"
    python convert.py $file bccvl
done

for file in NaRClim/NarCLIM_9sec/*.zip 
do
    echo "convert $file"
    python convert.py $file bccvl
done
