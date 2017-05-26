#!/usr/bin/env python
import os
import os.path
import zipfile
import glob
import json
import tempfile
import shutil
import sys
import numpy as np
from osgeo import gdal
import re
from collections import namedtuple

JSON_TEMPLATE = 'aust-substrate-fertility.json'

LAYER_MAP = {
    # NVIS Australian vegetation group
    # source directory file, ('source filename', dest filename)
    'fert': ('fert.FLT', 'inherent_rock_fertility_index.tif'),
    'geollmeanage': ('geollmeanage.FLT', 'mean_geological_age.tif'),
    'corg0': ('corg0.FLT', 'pre-european_annual_soil_organic_carbon.tif'),
    'ptotn0': ('ptotn0.FLT', 'pre-european_soil_phosphorus.tif')
}


def convert(src, dest):
    """convert .FLT files to .tif in dest
    """    
    print "Converting {}".format(src)

    #Use gdal_cal.py to convert FLT (Esri Grid Float format) file to geotif file
    ret = os.system(
            'gdal_calc.py -A {0} --outfile={1} --calc="A*(A>=-9999) - 9999*(A<-9999)" --co "COMPRESS=LZW" --co "TILED=YES" --NoDataValue=-9999'.format(src, dest)
        )
    if ret != 0:
        raise Exception(
            "can't gdal_cal.py {0} ({1})".format(src, ret)
        )

def copy_metadatajson(dest):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """
    md = json.load(open(JSON_TEMPLATE, 'r'))
    mdfile = open(os.path.join(dest, 'bccvl', 'metadata.json'), 'w')
    json.dump(md, mdfile, indent=4)
    mdfile.close()

def create_target_dir(destdir, destfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    root = os.path.join(destdir, destfile)
    os.mkdir(root)
    os.mkdir(os.path.join(root, 'data'))
    os.mkdir(os.path.join(root, 'bccvl'))
    return root


def zip_dataset(ziproot, dest):
    workdir = os.path.dirname(ziproot)
    zipdir = os.path.basename(ziproot)
    zipname = os.path.abspath(os.path.join(dest, zipdir + '.zip'))
    ret = os.system(
        'cd {0}; zip -r {1} {2}'.format(workdir, zipname, zipdir)
    )
    if ret != 0:
        raise Exception("can't zip {0} ({1})".format(ziproot, ret))


def main(argv):
    ziproot = None
    if len(argv) != 3:
        print "Usage: {0} <srcdir> <destdir>".format(argv[0])
        sys.exit(1)
    srcdir  = argv[1]
    destdir = argv[2]
    fname, ext = os.path.splitext(os.path.basename(srcdir))
    destfile = 'australian_substrate_fertility'
    try:
        ziproot = create_target_dir(destdir, destfile)
        for datasrc in LAYER_MAP:
            src_fragment, destfname = LAYER_MAP[datasrc]
            src_path = os.path.abspath(os.path.join(srcdir, datasrc, src_fragment))
            convert(src_path, os.path.join(ziproot, 'data', destfname))
        copy_metadatajson(ziproot)
        zip_dataset(ziproot, destdir)
    finally:
        if ziproot:
            pass
            shutil.rmtree(ziproot)

if __name__ == "__main__":
    main(sys.argv)
