#!/bin/bash


WORKDIR=/mnt/playground/$(basename ${PWD})
echo ${WORKDIR}
mkdir ${WORKDIR}


function update_metadata {
  python <<PY_SCRIPT
import sys, os.path, zipfile, json
workdir = "$WORKDIR"
srczip = "$1"
zip = zipfile.ZipFile(srczip)
base, _ = os.path.splitext(os.path.basename(srczip))
md = json.load(zip.open('/'.join([base, 'bccvl', 'metadata.json'])))
files = {}
for file in zip.namelist():
    if not file.endswith('.tif'):
        continue
    layer, _ = os.path.basename(file).rsplit('_', 1)
    files[file] = {'layer': layer}
md['files']= files
md["bccvl_metadata_version"] = "2014-09-01-01"
_, date = base.rsplit('_', 1)
year = date[:4]
md['temporal_coverage'] = {
    'start': year,
    'end': year
}
md['genre'] = 'Environmental'
if 'layers' in md:
    del md['layers']
destdir = os.path.join(workdir, base, 'bccvl')
if not os.path.exists(destdir):
    os.makedirs(destdir)
dest = os.path.join(destdir, 'metadata.json')
mdfile = open(dest, 'w')
json.dump(md, mdfile, indent=4) 
PY_SCRIPT
}

function update_zip_file {
    pushd ${WORKDIR}
    filebase=$(basename "$1")
    dirbase="${filebase%.*}"
    zip -r $1 $dirbase
    popd
}

for file in ${PWD}/bccvl/upload/*.zip ; do
    echo "convert metadata $file"
    update_metadata $file
    update_zip_file $file
done



