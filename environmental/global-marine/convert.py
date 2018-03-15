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
from collections import namedtuple
import calendar
from osgeo import gdal, ogr
import numpy as np

JSON_TEMPLATE = 'bccvl_marine-template-2017v2.json'

# Dataset layers
DATASET_INFO = {
    'Present.Surface.Temperature': {
       'title': "Global Marine Surface Data, Water Temperature (2000-2014), 5 arcmin (~10 km)",
       'data_type': "continuous"
    },
    'Present.Surface.Salinity': {
        'title': 'Global Marine Surface Data, Water Salinity (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous"
    },
    'Present.Surface.Current.Velocity': {
        'title': 'Global Marine Surface Data, Currents Velocity (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous"
    },
    'Present.Surface.Ice.thickness': {
        'title': 'Global Marine Surface Data, Ice Thickness (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous"
    },
    'Present.Surface.Ice.cover': {
        'title': 'Global Marine Surface Data, Sea Ice Concentration (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.Diffuse.attenuation': {
        'title': 'Global Marine Surface Data, Diffuse Attenuation (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.Cloud.cover': {
        'title': 'Global Marine Surface Data, Cloud Cover (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.Nitrate': {
        'title': 'Global Marine Surface Data, Nitrate (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.Phosphate': {
        'title': 'Global Marine Surface Data, Phosphate (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.Silicate': {
        'title': 'Global Marine Surface Data, Silicate (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.Dissolved.oxygen': {
        'title': 'Global Marine Surface Data, Dissolved Molecular Oxygen (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.Iron': {
        'title': 'Global Marine Surface Data, Iron (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.Calcite': {
        'title': 'Global Marine Surface Data, Calcite (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.Chlorophyll': {
        'title': 'Global Marine Surface Data, Chlorophyll (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.Phytoplankton': {
        'title': 'Global Marine Surface Data, Phytoplankton (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.Primary.productivity': {
        'title': 'Global Marine Surface Data, Primary Productivity (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.pH': {
        'title': 'Global Marine Surface Data, pH (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Present.Surface.Par': {
        'title': 'Global Marine Surface Data, Photosynthetically Available Radiation (2000-2014), 5 arcmin (~10 km)',
        'data_type': "continuous",
    }
}


def gen_metadatajson(dsname, src, dest):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """

    dsinfo = DATASET_INFO[dsname]
    md = json.load(open(src, 'r'))
    md['title'] = dsinfo['title']
    md['data_type'] = dsinfo['data_type']
    md[u'files'] = {}
    for filename in glob.glob(os.path.join(dest, '*', '*.tif')):
        base = os.path.basename(filename)
        base, _ = os.path.splitext(base)

        layer_id = base
        filename = filename[len(os.path.dirname(dest)):].lstrip('/')
        md[u'files'][filename] = {
            u'layer': layer_id,
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



def convert_dataset(srcfolder, dsname):

    # Get the layers from the zip files
    destdir = dsname
    dsglob = dsname + '*.zip'
    dsttmpdir = tempfile.mkdtemp()
    ziproot = create_target_dir(dsttmpdir, destdir)
    for zipfile in glob.glob(os.path.join(srcfolder, dsglob)):
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

    # add metadata.json for the dataset
    gen_metadatajson(dsname, JSON_TEMPLATE, ziproot)
    return dsttmpdir

def main(argv):
    srcfolder = 'source/Marine.Present.Surface.tif'
    destfolder = 'bccvl'

    tmpdest = None
    for dsname in DATASET_INFO.keys():
        try:
            tmpdest = convert_dataset(srcfolder, dsname)

            # ziproot = tmpdest/dsname
            zip_dataset(os.path.join(tmpdest, dsname),
                        destfolder)
        finally:
            if tmpdest:
                shutil.rmtree(tmpdest)


if __name__ == "__main__":
    main(sys.argv)
