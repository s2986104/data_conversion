#!/usr/bin/env python
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

JSON_TEMPLATE = 'bccvl_marine-template-2017v2.json'

# Layer Depth
LAYER_DEPTH = ['Surface']

# Dataset layers
DATASET_INFO = {
    'Surface.Temperature': {
       'title': "Global Marine Surface Data, Water Temperature {0}, 5 arcmin (~10 km)",
       'data_type': "continuous"
    },
    'Surface.Salinity': {
        'title': 'Global Marine Surface Data, Water Salinity {0}, 5 arcmin (~10 km)',
        'data_type': "continuous"
    },
    'Surface.Current.Velocity': {
        'title': 'Global Marine Surface Data, Currents Velocity {0}, 5 arcmin (~10 km)',
        'data_type': "continuous"
    },
    'Surface.Ice.thickness': {
        'title': 'Global Marine Surface Data, Ice Thickness {0}, 5 arcmin (~10 km)',
        'data_type': "continuous"
    },
    'Surface.Ice.cover': {
        'title': 'Global Marine Surface Data, Sea Ice Concentration {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.Diffuse.attenuation': {
        'title': 'Global Marine Surface Data, Diffuse Attenuation {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.Cloud.cover': {
        'title': 'Global Marine Surface Data, Cloud Cover {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.Nitrate': {
        'title': 'Global Marine Surface Data, Nitrate {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.Phosphate': {
        'title': 'Global Marine Surface Data, Phosphate {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.Silicate': {
        'title': 'Global Marine Surface Data, Silicate {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.Dissolved.oxygen': {
        'title': 'Global Marine Surface Data, Dissolved Molecular Oxygen {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.Iron': {
        'title': 'Global Marine Surface Data, Iron {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.Calcite': {
        'title': 'Global Marine Surface Data, Calcite {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.Chlorophyll': {
        'title': 'Global Marine Surface Data, Chlorophyll {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.Phytoplankton': {
        'title': 'Global Marine Surface Data, Phytoplankton {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.Primary.productivity': {
        'title': 'Global Marine Surface Data, Primary Productivity {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.pH': {
        'title': 'Global Marine Surface Data, pH {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    },
    'Surface.Par': {
        'title': 'Global Marine Surface Data, Photosynthetically Available Radiation {0}, 5 arcmin (~10 km)',
        'data_type': "continuous",
    }
}

# Layer period
LAYER_PERIOD = {
    'current': {
        'period': '2000-2014',
        'scenerio': ['na'],
        'source': 'source/Marine.Present.Surface.tif',
        'variables': DATASET_INFO.keys()
    },
    '2050': {
        'period': '2040-2050',
        'scenerio': ['RCP26', 'RCP45', 'RCP60', 'RCP85'],
        'source': 'source/Marine.Future.Surface.tif',
        'variables': ['Surface.Temperature', 'Surface.Salinity', 'Surface.Current.Velocity', 'Surface.Ice.thickness']
    },
    '2100': {
        'period': '2090-2100',
        'scenerio': ['RCP26', 'RCP45', 'RCP60', 'RCP85'],
        'source': 'source/Marine.Future.Surface.tif',
        'variables': ['Surface.Temperature', 'Surface.Salinity', 'Surface.Current.Velocity', 'Surface.Ice.thickness']
    }
}


def gen_metadatajson(dsid, src, dest, scenerio, period, layer_depth='Surface'):
    """read metadata template and populate rest of fields
       and write to dest + '/bccvl/metadata.json'
    """
    dsinfo = DATASET_INFO[dsid]
    md = json.load(open(src, 'r'))
    if period == 'current':
        period_str = '({0})'.format(LAYER_PERIOD[period].get('period'))
        md['temporal_coverage'] = {'start': '2000', 'end': '2014'}
    else:
        sc = re.match("([a-z]+)([0-9]+)", scenerio, re.I).groups()
        period_str = '({0}), {1}'.format(LAYER_PERIOD[period].get('period'), " ".join(sc))
        start = '2040' if period == '2050' else '2090'
        md['temporal_coverage'] = {'start': start, 'end': period}

    md['title'] = dsinfo['title'].format(period_str)
    md['data_type'] = dsinfo['data_type']
    md[u'files'] = {}
    for filename in glob.glob(os.path.join(dest, '*', '*.tif')):
        base = os.path.basename(filename)
        base, _ = os.path.splitext(base)

        # get rid of the preceding characters to get the layer id
        pos = base.find(layer_depth)
        if pos > 0:
            base = base[pos:]

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



def convert_dataset(srcfolder, dsname, dsid, scenerio, period):

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
    gen_metadatajson(dsid, JSON_TEMPLATE, ziproot, scenerio, period)
    return dsttmpdir

def main(argv):
    parser = argparse.ArgumentParser(description='Convert Global Marine datasets')
    parser.add_argument('--period', type=str, choices=['current', '2050', '2100'], help='dataset period')
    params = vars(parser.parse_args(argv[1:]))
    period_list = [params.get('period')] if params.get('type') is not None else ['current', '2050', '2100']

    destfolder = 'bccvl'

    tmpdest = None
    for period in period_list:
        srcfolder = LAYER_PERIOD[period].get('source')
        for scenerio in LAYER_PERIOD[period].get('scenerio'):
            header = 'Present' if period == 'current' else '{0}.{1}'.format(period, scenerio)
            for dsid in LAYER_PERIOD[period].get('variables'):
                # dataset filename
                dsname = "{0}.{1}".format(header, dsid)
                print dsname, scenerio, header, srcfolder, period
                try:
                    tmpdest = convert_dataset(srcfolder, dsname, dsid, scenerio, period)

                    print tmpdest

                    # ziproot = tmpdest/dsname
                    zip_dataset(os.path.join(tmpdest, dsname),
                                destfolder)
                finally:
                    if tmpdest:
                        shutil.rmtree(tmpdest)


if __name__ == "__main__":
    main(sys.argv)
