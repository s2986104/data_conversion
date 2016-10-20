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

JSON_TEMPLATE = 'nvis.template.json'

LAYER_MAP = {
    # NVIS Australian vegetation group
    # source directory file, ('source fragment', dest filename)
    'GRID_NVIS4_2_AUST_EXT_MVG': ('aus4_2e_mvg', 'nvis_present_vegetation_groups.tif'),
    'GRID_NVIS4_2_AUST_PRE_MVG': ('aus4_2p_mvg', 'nvis_pre-1750_vegetation_groups.tif')
}

def gdal_translate(src, dest):
    """Use gdal_translate to copy file from src to dest"""
    # gdal_translate does not include the RAT when -a_srs is specified.
    # As a hack, 1st convert with -a_srs, then convert without -a_srs to generate the RAT file.
    destfname = os.path.basename(dest)
    destdir = os.path.dirname(dest)

    ret = os.system('gdal_translate -of GTiff {0} {1}'.format(src, os.path.join(destdir, 'tempfilet1.tif')))
    os.remove(os.path.join(destdir, 'tempfilet1.tif'))
    ratfile = os.path.join(destdir, 'tempfilet1.tif.aux.xml')
    
    if ret != 0 or not os.path.isfile(ratfile):
        raise Exception("fail to generate RAT file for {0} ({1})".format(src, ret))

    ret = os.system('gdal_translate -of GTiff -a_srs epsg:3577 -co "COMPRESS=LZW" -co "TILED=YES" {0} {1}'
                    .format(src, dest))
    # rename RAT file generated
    os.rename(ratfile, dest + '.aux.xml')

    if ret != 0:
        raise Exception("can't gdal_translate {0} ({1})".format(src, ret))

def convert(src, dest):
    """convert .ovr files to .tif in dest
    """    
    print "Converting {}".format(src)
    gdal_translate(src, dest)

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
    destfile = 'nvis_major_vegetation_groups'
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
