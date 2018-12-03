#!/usr/bin/env python
import os
import os.path
import zipfile
import glob
import json
import tempfile
import shutil
import sys
import re

CURRENT_TITLE = u'WorldClim Current Conditions (1950-2000), {resolution}'
FUTURE_TITLE = u'WorldClim, Climate Projection, {resolution}'
JSON_TEMPLATE = 'worldclim.template.json'
SWIFT_CONTAINER= 'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/worldclim_layers'


RESOLUTION = {
    '30s': '30 arcsec',
    '2-5m': '2.5 arcmin',
    '5m': '5 arcmin',
    '10m': '10 arcmin'
}

layermds = []   # for layer md
datasetmds = []     # for dataset md

def gen_metadatajson(template, datadir, swiftcontainer):
    """read metadata template and populate rest of fields
    and write to metadata.json'
    """
    base = os.path.basename(datadir)
    basedir = os.path.dirname(datadir)

    # parse info from filename
    # check for future climate dataset:
    md = json.load(open(template, 'r'))

    if 'current' in datadir.lower():
        md[u'genre'] = 'DataGenreCC'
    else:
        md[u'genre'] = 'DataGenreFC'

    m = re.match(r'([\w.]*)_([\w-]*)_(\d*)_([\w-]*)_(\w*)', base)
    if m:
        md[u'temporal_coverage'][u'start'] = unicode(m.group(3))
        md[u'temporal_coverage'][u'end'] = unicode(m.group(3))
        md[u'emsc'] = unicode(m.group(2))
        md[u'gcm'] = unicode(m.group(1))
        md[u'resolution'] = RESOLUTION[m.group(4)]  #30s, 2-5m, 5m, 10m
        md[u'type'] = m.group(5)    # i.e. bioclim, prec, tmax, tmin,
    else:
        # can only be current
        m = re.match(r'(\w*)_([\w-]*)_(\w*)', base)
        if m:
            md[u'temporal_coverage'][u'start'] = u'1950'
            md[u'temporal_coverage'][u'end'] = u'2000'
            md[u'resolution'] = RESOLUTION[m.group(2)]
            md[u'type'] = m.group(3)    # i.e. bioclim, prec, tmax, tmin, alt
        else:
            raise Exception("Fail to parse ", base)

    layers = []     # layers for dataset md
    # layer specific metadata
    for filename in glob.glob(os.path.join(datadir, '*.tif')):
        filename = os.path.basename(filename)
        md[u'filename'] = unicode(filename)
        md[u'url'] = unicode(os.path.join(swiftcontainer, base, filename))
        md[u'layer'] = unicode(filename[len(base)+1:-4])
        md[u'data_type'] = md['data_type']    # This should be specific to layers i.e. in md['files']
        layers.append(md['url'])
        layermds.append(md)

def gen_dataset_metadata(template, category, genre, resolution):
    md = json.load(open(template, 'r'))
    ds_md = {}
    ds_md[u'category'] = category
    ds_md[u'genre'] = genre
    ds_md[u'resolution'] = resolution
    ds_md[u'acknowledgement'] = md[u'acknowledgement']
    ds_md[u'external_url'] = md[u'external_url']
    ds_md[u'license'] = md[u'license']
    ds_md[u'bounding_box'] = md[u'bounding_box']
    ds_md[u'layers'] = [ lyr['url'] for lyr in layermds if lyr['genre'] == genre and lyr['resolution'] == resolution ]
    if genre == 'DataGenreFC':
        ds_md[u'title'] = FUTURE_TITLE.format(resolution=resolution)
    else:
        ds_md[u'title'] = CURRENT_TITLE.format(resolution=resolution)
    return ds_md


def main(argv):
    if len(argv) != 2:
        print "Usage: {0} <srcdir>".format(argv[0])
        sys.exit(1)
    srcdir = argv[1]

    resolution = '5 arcmin'
    resol = [key for key in RESOLUTION.keys() if RESOLUTION[key] == resolution]
    print "Resolution = ", resol
    category = ["climate", "topography"]
    for subdir in ('current-layers', 'future-layers'):
        for dataset in glob.glob(os.path.join(srcdir, subdir, '*_' + resol[0] + '_*')):
            print "Processing ", dataset
            gen_metadatajson(JSON_TEMPLATE, dataset, SWIFT_CONTAINER)
    
    for genre in ("DataGenreCC", "DataGenreFC"):
        dsmd = gen_dataset_metadata(JSON_TEMPLATE, category, genre, resolution)
        if dsmd:
            datasetmds.append(dsmd)

    # save layer metadata to file
    prefix = 'worldclim_' + resolution.replace(' ', '')
    with open(os.path.join(srcdir, prefix + '_layer_metadata.json'), 'w') as mdfile:
        json.dump({"type": "layer", "data": layermds}, mdfile, indent=4)

    # save dataset metadata to file
    with open(os.path.join(srcdir, prefix + '_dataset_metadata.json'), 'w') as dsmdfile:
        json.dump({"type": "dataset", "data": datasetmds}, dsmdfile, indent=4)

    # save collection


if __name__ == "__main__":
    main(sys.argv)
