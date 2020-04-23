files="data.json datasets.json collection.json"
folders=("national-dynamic-land-cover" "national-soil-grids")
datastores=("national-dynamic-land-cover_layers" "national_soil_grids_layers")
for i in ${!datastores[@]}
do
  datastore=${datastores[$i]}
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
