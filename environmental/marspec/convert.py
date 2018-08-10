#!/usr/bin/env python
# coding: latin-1
import os
import os.path
import zipfile
import glob
import json
import tempfile
import shutil
import sys
import argparse
import re
from collections import namedtuple
import calendar
from osgeo import gdal, ogr
import numpy as np

JSON_TEMPLATE = 'bccvl_marspec-template-2018v1.json'

def gen_metadatajson(src, dest):
    """read metadata template and populate rest of fields
       and write to dest + '/bccvl/metadata.json'
    """
    md = json.load(open(src, 'r'))
    md['temporal_coverage'] = {'start': '1955', 'end': '2010'}
    md['title'] = 'Global Marine Data, Bathymetry (1955-2010), 5 arcmin (~10 km)'
    md['data_type'] = 'continuous'
    md['dataset_version'] = 'v1 (2012)'
    md['external_url'] = 'http://marspec.weebly.com/modern-data.html'
    md['acknowledgement'] = ['Sbrocco, E. J. and Barber, P. H. (2013) MARSPEC: Ocean climate layers for marine spatial ecology. Ecology 94:979. http://dx.doi.org/10.1890/12-1358.1', 
                             'Becker, J. J., D. T. Sandwell, W. H. F. Smith, J. Braud, B. Binder, J. Depner, D. Fabre, J. Factor, S. Ingalls, S-H. Kim, R. Ladner, K. Marks, S. Nelson, A. Pharaoh, R. Trimmer, J. Von Rosenberg, G. Wallace, and P. Weatherall. 2009. Global Bathymetry and Elevation Data at 30 Arc Seconds Resolution: SRTM30_PLUS. Marine Geodesy 32:355–371.']

    
    md[u'files'] = {}
    for filename in glob.glob(os.path.join(dest, '*', '*.tif')):
        filename = filename[len(os.path.dirname(dest)):].lstrip('/')
        md[u'files'][filename] = {
            u'layer': 'Depth',
        }   
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


def unpack(zipname, path):
    """unpack zipfile to path
    """
    zipf = zipfile.ZipFile(zipname, 'r')
    zipf.extractall(path)


def unzip_dataset(dsfile):
    """unzip source dataset and return unzip location
    """
    tmpdir = tempfile.mkdtemp()
    try:
        unpack(dsfile, tmpdir)
    except:
        shutil.rmtree(tmpdir)
        raise
    return tmpdir

def _conver_dataset(dsname, srcfolder, dsglob, ziproot):
    for zipfile in glob.glob(os.path.join(srcfolder, dsglob)):
        srctmpdir = None
        try:
            print "converting ", dsname, zipfile
            srctmpdir = unzip_dataset(zipfile)
            
            # find all tif files in srctmpdir:
            for tiffile in glob.glob(os.path.join(srctmpdir, '*.tif')):
                # open dataset
                ds = gdal.Open(tiffile)
                # create new dataset in ziproot/data
                driver = ds.GetDriver()
                tifbase = os.path.basename(tiffile)
                newds = driver.CreateCopy(os.path.join(ziproot, 'data', tifbase),
                                          ds, strict=0,
                                          options=['TILED=YES',
                                                   'COMPRESS=LZW',
                                                   'PROFILE=GDALGeoTIFF'
                                                   ]
                )
                # generate band stats
                band = newds.GetRasterBand(1)
                band.ComputeStatistics(False)
                newds.FlushCache()
                ds = None
        except Exception as e:
            print "Error:", e
        finally:
            if srctmpdir:
                shutil.rmtree(srctmpdir)


def convert_dataset(srcfolder, dsname):
    # Get the layers from the zip files and cobert current layers
    destdir = dsname
    dsglob = dsname + '*.zip'
    dsttmpdir = tempfile.mkdtemp()
    ziproot = create_target_dir(dsttmpdir, destdir)
    _conver_dataset(dsname, srcfolder, dsglob, ziproot)

    # add metadata.json for the dataset
    gen_metadatajson(JSON_TEMPLATE, ziproot)
    return dsttmpdir

def main(argv):
    parser = argparse.ArgumentParser(description='Convert MARSPEC Marine datasets')
    parser.add_argument('--bathymetry', action='store_true', default=True, help='bathymetry dataset')

    params = vars(parser.parse_args(argv[1:]))
    bathymetry = params.get('bathymetry', False)

    destfolder = 'bccvl'
    tmpdest = None
    
    if bathymetry:
        srcfolder = 'source/bathymetry'
        period = '1955-2010'
        dsname = 'bathymetry_5m'
        try:
            tmpdest = convert_dataset(srcfolder, dsname)

            # ziproot = tmpdest/dsname
            zip_dataset(os.path.join(tmpdest, dsname),
                        destfolder)

        finally:
            if tmpdest:
                shutil.rmtree(tmpdest)
        return


if __name__ == "__main__":
    main(sys.argv)
