#!/bin/bash

export FILEPREFIX=multi_res_valley_bottom_flat
SRCDIR=/mnt/collection/datasets/environmental/multi-resolution-valley-bottom-flatness
SRCZIP=Multi-resolution_Valley_Bottom_Flatness__MrVBF__3__resolution_.zip
export TARGETDIR=/mnt/collection/datasets/environmental/multi-resolution-valley-bottom-flatness
WORKDIR=/mnt/playground
VRT=${FILEPREFIX}.vrt
export OUTTIF=${TARGETDIR}/${FILEPREFIX}.tif
export MJSONIN=${SRCDIR}/bccvl_metadata_mrvbf-2014062401.json

if [ ! -d $WORKDIR ] ; then
  mkdir -p $WORKDIR
fi

pushd $WORKDIR

if [ ! -d MrVBF_3s ] ; then
  unzip ${SRCDIR}/source/${SRCZIP}
fi

# Remove overview images
#rm MrVBF_3s/mrvbf6g-a5_3s_median/tiles/*/*/*.rrd
#rm MrVBF_3s/mrvbf6g-a5_3s_median/tiles/*/*/*.aux

# generate vrt
gdalbuildvrt ${VRT} MrVBF_3s/mrvbf6g-a5_3s_median/tiles/*/*/e1*

if [ ! -e ${OUTTIF} ] ; then 
    TIFFOPTS='-co TILED=YES -co COMPRESS=DEFLATE -co ZLEVEL=9'
    gdal_translate -of GTiff ${TIFFOPTS} ${VRT} ${OUTTIF}
fi

rm -fr MrVBF_3s
rm ${VRT}

# fixup metadata:

python <<PY_SCRIPT
import os
from osgeo import gdal, osr
ds = gdal.Open(os.environ['OUTTIF'], gdal.GA_Update)
sr = osr.SpatialReference()
# Horizontal WGS84 + Vertical EPG96
sr.SetFromUserInput('EPSG:4326+5773')
ds.SetProjection(sr.ExportToWkt())
rb = ds.GetRasterBand(1)
rb.SetColorInterpretation(gdal.GCI_Undefined)
stats = rb.ComputeStatistics(False)
rb.SetStatistics(*stats)
rat = gdal.RasterAttributeTable()
rat.CreateColumn('VALUE', gdal.GFT_Integer, gdal.GFU_MinMax)
rat.CreateColumn('Threshold Slope (%)', gdal.GFT_String, gdal.GFU_Generic)
rat.CreateColumn('Resolution (approx meter)', gdal.GFT_Integer, gdal.GFU_Generic)
rat.CreateColumn('Interpretation', gdal.GFT_String, gdal.GFU_Name)
data = (
    (0, '', 30, 'Erosional'),
    (1, '16', 30, 'Small hillside deposit'),
    (2, '8', 30, 'Narrow valley floor'),
    (3, '4', 90, ''),
    (4, '2', 270, 'Valley floor'),
    (5, '1', 800, 'Extensive valley floor'),
    (6, '0.5', 2400, ''),
    (7, '0.25', 7200, 'Depositional basin'),
    (8, '0.125', 22000, ''),
    (9, '0.0625', 66000, 'Extensive depositional basin')
)
for value, slope, resolution, interpr in data:
    rat.SetValueAsInt(value, 0, value)
    rat.SetValueAsString(value, 1, slope)
    rat.SetValueAsInt(value, 2, resolution)
    rat.SetValueAsString(value, 3, interpr)
rb.SetDefaultRAT(rat)
PY_SCRIPT


# zip result
cd ${TARGETDIR}/bccvl
mkdir -p ${FILEPREFIX}/data
mkdir -p ${FILEPREFIX}/bccvl
mv ${OUTTIF} ${FILEPREFIX}/data
mv ${OUTTIF}.aux.xml ${FILEPREFIX}/data
# create metadata.json
export MJSONOUT=${FILEPREFIX}/bccvl/metadata.json
python <<PY_SCRIPT
import json, os
js = open(os.environ['MJSONIN'], 'r')
md = json.load(js)
md['layers'] = [{
    'title': md['title'],
    'file_pattern': os.environ['FILEPREFIX'],
}]
js = open(os.environ['MJSONOUT'], 'w')
json.dump(md, js, indent=4)
js.close()
PY_SCRIPT

zip -r ${FILEPREFIX}.zip ${FILEPREFIX}

rm -fr ${FILEPREFIX}

# return to start dir
popd

