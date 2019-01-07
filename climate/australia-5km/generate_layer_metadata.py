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

CURRENT_CITATION = u'Jones, D. A., Wang, W., & Fawcett, R. (2009). High-quality spatial climate data-sets for Australia. Australian Meteorological and Oceanographic Journal, 58(4), 233.'
CURRENT_TITLE = u'Australia, Current Climate (1976-2005), 2.5 arcmin (~5 km)'
FUTURE_TITLE = u'Australia, Climate Projection, {resolution}'
JSON_TEMPLATE = 'bccvl_australia_5km.template.json'
SWIFT_CONTAINER= 'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/australia_5km_layers'

EMSC_TITLE = { 
        "RCP2.6": "RCP3PD",
        "RCP4.5": "RCP45",
        "RCP6.0": "RCP6.0",
        "RCP8.5": "RCP85",
        "SRESA1B": "SRESA1B",
        "SRESA1FI": "SRESA1FI",
       "SRESA2": "SRESA2",
       "SRESB1": "SRESB1",
       "SRESB2": "SRESB2"
}

layermds = []   # for layer md
datasetmds = []     # for dataset md

def gen_metadatajson(template, datadir, swiftcontainer, resolution):
    """read metadata template and populate rest of fields
    and write to metadata.json'
    """
    base = os.path.basename(datadir)
    basedir = os.path.dirname(datadir)

    # parse info from filename
    # check for future climate dataset:
    md = json.load(open(template, 'r'))
    del md['files']

    if 'current' in datadir.lower():
        md[u'genre'] = 'DataGenreCC'
    else:
        md[u'genre'] = 'DataGenreFC'
    md[u'resolution'] = resolution

    m = re.match(r'([\w.]*)_([\w-]*)_(\d*)', base)
    if m:
        md[u'temporal_coverage'][u'start'] = unicode(m.group(3))
        md[u'temporal_coverage'][u'end'] = unicode(m.group(3))
        md[u'emsc'] = unicode(m.group(1))
        md[u'gcm'] = unicode(m.group(2))
    else:
        # can only be current
        md[u'temporal_coverage'][u'start'] = u'1976'
        md[u'temporal_coverage'][u'end'] = u'2005'
        md[u'acknowledgement'] = CURRENT_CITATION
        md[u'external_url'] = u''

    layers = []     # layers for dataset md
    # layer specific metadata
    for filename in glob.glob(os.path.join(datadir, '*.tif')):
        filename = os.path.basename(filename)
        md2 = md.copy()
        md2[u'filename'] = unicode(filename)
        md2[u'url'] = unicode(os.path.join(swiftcontainer, base, filename))
        md2[u'layer'] = unicode(filename[len(base)+1:-4])
        md2[u'data_type'] = md['data_type']    # This should be specific to layers i.e. in md['files']
        layers.append(md2['url'])
        layermds.append(md2)

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
    ds_md[u'layers'] = [ lyr['url'] for lyr in layermds if lyr['genre'] == genre ]
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

    category = u"climate"
    resolution = u"2.5 arcmin"
    for dataset in glob.glob(os.path.join(srcdir, '*')):
        gen_metadatajson(JSON_TEMPLATE, dataset, SWIFT_CONTAINER, resolution)

    for genre in ("DataGenreCC", "DataGenreFC"):
        dsmd = gen_dataset_metadata(JSON_TEMPLATE, category, genre, resolution)
        if dsmd:
            datasetmds.append(dsmd)

    # save layer metadata to file
    with open(os.path.join(srcdir, 'layer_metadata.json'), 'w') as mdfile:
        json.dump({"type": "layer", "data": layermds}, mdfile, indent=4)

    # save dataset metadata to file
    with open(os.path.join(srcdir, 'dataset_metadata.json'), 'w') as dsmdfile:
        json.dump({"type": "dataset", "data": datasetmds}, dsmdfile, indent=4)

    # save collection


if __name__ == "__main__":
    main(sys.argv)
