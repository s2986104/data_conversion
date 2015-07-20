#!/bin/bash


WORKDIR=/mnt/playground/$(basename ${PWD})
echo ${WORKDIR}
mkdir ${WORKDIR}


function update_metadata {
  python <<PY_SCRIPT
import sys, os.path, zipfile, json
LAYER_MAP = {
    'bioclim_01.tif': 'B01',
    'bioclim_02.tif': 'B02',
    'bioclim_03.tif': 'B03',
    'bioclim_04.tif': 'B04',
    'bioclim_05.tif': 'B05',
    'bioclim_06.tif': 'B06',
    'bioclim_07.tif': 'B07',
    'bioclim_08.tif': 'B08',
    'bioclim_09.tif': 'B09',
    'bioclim_10.tif': 'B10',
    'bioclim_11.tif': 'B11',
    'bioclim_12.tif': 'B12',
    'bioclim_13.tif': 'B13',
    'bioclim_14.tif': 'B14',
    'bioclim_15.tif': 'B15',
    'bioclim_16.tif': 'B16',
    'bioclim_17.tif': 'B17',
    'bioclim_18.tif': 'B18',
    'bioclim_19.tif': 'B19',
}
workdir = "$WORKDIR"
srczip = "$1"
zip = zipfile.ZipFile(srczip)
base, _ = os.path.splitext(os.path.basename(srczip))
md = json.load(zip.open('/'.join([base, 'bccvl', 'metadata.json'])))
files = {}
for file in zip.namelist():
    if os.path.basename(file) not in LAYER_MAP:
        continue
    layer = LAYER_MAP[os.path.basename(file)]
    files[file] = {'layer': layer}
md['files']= files
md["bccvl_metadata_version"] = "2014-09-01-01"
if 'current' in srczip.lower():
    md['genre'] = 'Climate'
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
    echo "Update ZIP: $1 $dirsbase"
    zip -r $1 $dirbase
    popd
}

for file in ${PWD}/bccvl/*.zip ; do
    echo "convert metadata $file"
    update_metadata $file
    update_zip_file $file
done



