#!/bin/bash

files="data.json datasets.json collection.json"
datastores=("aus-csiro_layers" "aus-csiro_layers" "aus-csiro_layers" "aus-csiro_layers" "aus-veg-fpar_layers" "aus-veg-gpp_layers")
destpaths=("aus-clim-csiro" "aus-enviro-topography" "aus-enviro-substrate" "aus-enviro-vegetation" "aus-veg-fpar" "aus-veg-gpp")

for i in ${!datastores[@]}
do
  datastore=${datastores[$i]}
  destpath=${destpaths[$i]}
  for file in $files
  do
    # pattern: swift upload <dest-datastore> <src-dest-data-path>
    echo "uploading to $datastore -> $destpath/$file"
    cmd="swift upload $datastore $destpath/$file"
    $cmd
  done
done

#files="data.json datasets.json collection.json"
folders=("national-dynamic-land-cover" "national-soil-grids")
datastores=("national-dynamic-land-cover_layers" "national_soil_grids_layers")
for i in ${!datastores[@]}
do
  datastore=${datastores[$i]}
  destpath=${destpaths[$i]}
  cd ${folders[$i]}
  for file in $files
  do
    # pattern: swift upload <dest-datastore> <src-dest-data-path>
    echo "uploading to $datastore -> $file"
    cmd="swift upload $datastore $file"
    $cmd
  done
  cd ..
done