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

CURRENT_CITATION = u'"Hutchinson M, Kesteven J, Xu T (2014) Monthly climate data: ANUClimate 1.0, 0.01 degree, Australian Coverage, 1976-2005. Australian National University, Canberra, Australia. Made available by the Ecosystem Modelling and Scaling Infrastructure (eMAST, http://www.emast.org.au) of the Terrestrial Ecosystem Research Network (TERN, http://www.tern.org.au).'
CURRENT_TITLE = u'ANUClim (Australia), Current Climate {month} (1976-2005) , 30arcsec (~1km)'
JSON_TEMPLATE = 'anuclim.template.json'

LAYER_MAP = {
    'prec_01.tif': 'PR1',
    'prec_02.tif': 'PR2',
    'prec_03.tif': 'PR3',
    'prec_04.tif': 'PR4',
    'prec_05.tif': 'PR5',
    'prec_06.tif': 'PR6',
    'prec_07.tif': 'PR7',
    'prec_08.tif': 'PR8',
    'prec_09.tif': 'PR9',
    'prec_10.tif': 'PR10',
    'prec_11.tif': 'PR11',
    'prec_12.tif': 'PR12',
    'tmax_01.tif': 'TX1',
    'tmax_02.tif': 'TX2',
    'tmax_03.tif': 'TX3',
    'tmax_04.tif': 'TX4',
    'tmax_05.tif': 'TX5',
    'tmax_06.tif': 'TX6',
    'tmax_07.tif': 'TX7',
    'tmax_08.tif': 'TX8',
    'tmax_09.tif': 'TX9',
    'tmax_10.tif': 'TX10',
    'tmax_11.tif': 'TX11',
    'tmax_12.tif': 'TX12',
    'tmin_01.tif': 'TN1',
    'tmin_02.tif': 'TN2',
    'tmin_03.tif': 'TN3',
    'tmin_04.tif': 'TN4',
    'tmin_05.tif': 'TN5',
    'tmin_06.tif': 'TN6',
    'tmin_07.tif': 'TN7',
    'tmin_08.tif': 'TN8',
    'tmin_09.tif': 'TN9',
    'tmin_10.tif': 'TN10',
    'tmin_11.tif': 'TN11',
    'tmin_12.tif': 'TN12',
    'evap_01.tif': 'mon_evap1',
    'evap_02.tif': 'mon_evap2',
    'evap_03.tif': 'mon_evap3',
    'evap_04.tif': 'mon_evap4',
    'evap_05.tif': 'mon_evap5',
    'evap_06.tif': 'mon_evap6',
    'evap_07.tif': 'mon_evap7',
    'evap_08.tif': 'mon_evap8',
    'evap_09.tif': 'mon_evap9',
    'evap_10.tif': 'mon_evap10',
    'evap_11.tif': 'mon_evap11',
    'evap_12.tif': 'mon_evap12',
    'vapp_01.tif': 'mon_vapp1',
    'vapp_02.tif': 'mon_vapp2',
    'vapp_03.tif': 'mon_vapp3',
    'vapp_04.tif': 'mon_vapp4',
    'vapp_05.tif': 'mon_vapp5',
    'vapp_06.tif': 'mon_vapp6',
    'vapp_07.tif': 'mon_vapp7',
    'vapp_08.tif': 'mon_vapp8',
    'vapp_09.tif': 'mon_vapp9',
    'vapp_10.tif': 'mon_vapp10',
    'vapp_11.tif': 'mon_vapp11',
    'vapp_12.tif': 'mon_vapp12',
}

MONTH_LIST = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
SOURCE_DATASETS = [('tmax', 'tif'), ('tmin', 'tif'), ('prec', 'tif'), ('vapp', 'asc.gz'), ('evap', 'asc.gz')]

def ungz(filename):
    """gunzip given filename.
    """
    ret = os.system('gunzip {0}'.format(filename))
    if ret != 0:
        raise Exception("can't gunzip {0} ({1})".format(filename, ret))


def unpack(zipname, path):
    """unpack zipfile to path
    """
    zipf = zipfile.ZipFile(zipname, 'r')
    zipf.extractall(path)

def gdal_translate(src, dest):
    """Use gdal_translate to copy file from src to dest"""
    ret = os.system('gdal_translate -of GTiff {0} {1}'.format(src, dest))

    if ret != 0:
        raise Exception("can't gdal_translate {0} ({1})".format(src, ret))

def convert(srcdir, ziproot, month):
    """copy tmax, tmin and prep files and convert if necessary to zip preparation dir.
    """
    for layer, ext in SOURCE_DATASETS:
        if os.name == 'nt':
            srcfile = "/".join([srcdir, layer, '{0}_{1:02d}.{2}'.format(layer, month, ext)])
        else:
            srcfile = os.path.join(srcdir, layer, '{0}_{1:02d}.{2}'.format(layer, month, ext))

        if ext == 'asc.gz':
            ungz(srcfile)
            srcfile = srcfile[:-len('.gz')]

        # convert to tiff file
        destfile = '{0}_{1:02d}.tif'.format(layer, month)
        gdal_translate(srcfile, os.path.join(ziproot, 'data', destfile))


def gen_metadatajson(template, dest, month):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """
    base = os.path.basename(dest)
    # parse info from filename
    # check for future climate dataset:
    md = json.load(open(template, 'r'))
    md[u'temporal_coverage'][u'start'] = u'1976'
    md[u'temporal_coverage'][u'end'] = u'2005'
    md[u'title'] = CURRENT_TITLE.format(month=month)
    md[u'acknowledgement'] = CURRENT_CITATION
    md[u'external_url'] = u'http://www.emast.org.au/our-infrastructure/observations/anuclimate_data/'
    md['genre'] = 'Climate'
    md['license'] = 'Creative Commons Attribution 4.0 AU ihttp://creativecommons.org/licenses/by/4.0/'
    md['files'] = {}
    md['bounding_box'] = {
        "top": "-9.0050000",
        "right": "153.9950000",
        "bottom": "-43.7450000",
        "left": "112.8950000"
    }

    # Update layer info
    for filename in glob.glob(os.path.join(dest, '*', '*.tif')):
        filename = filename[len(dest):].lstrip('/')
	md['files'][filename] = {
            'layer': LAYER_MAP[os.path.basename(filename)]
        }

    mdfile = open(os.path.join(dest, 'bccvl', 'metadata.json'), 'w')
    json.dump(md, mdfile, indent=4)
    mdfile.close()


def unzip_dataset(dsfile, tmpdir=None):
    """unzip source dataset and return unzip location
    """
    if tmpdir is None:
        tmpdir = tempfile.mkdtemp()
    else:
        if os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)
        os.makedirs(tmpdir)

    try:
        print "unpack dataset %s" %dsfile
        unpack(dsfile, tmpdir)
    except:
        shutil.rmtree(tmpdir)
        raise
    return tmpdir


def create_target_dir(destdir, resolution, month):
    """create zip folder structure in tmp location.
    return root folder
    """
    basename = 'anuclim_{}_{}'.format(resolution, MONTH_LIST[month][:3])
    root = os.path.join(destdir, basename)
    os.mkdir(root)
    os.mkdir(os.path.join(root, 'data'))
    os.mkdir(os.path.join(root, 'bccvl'))
    return root


def zipbccvldataset(ziproot, dest):
    try:
        pwd = os.getcwd()
        workdir = os.path.dirname(ziproot)
        zipdir = os.path.basename(ziproot)
        os.chdir(workdir)
        shutil.make_archive(zipdir, "zip", zipdir)
        shutil.rmtree(zipdir)
        os.chdir(pwd)
    except:
        raise Exception("can't zip {0}".format(ziproot))


def main(argv):
    ziproot = None
    srctmpdir = {}
    try:
        if len(argv) != 3:
            print "Usage: {0} <srcdir> <destdir>".format(argv[0])
            sys.exit(1)
        srcdir = argv[1]
        dest = argv[2]

        # source contains 5 zipped datasets: tmax, tmin, prec, vapp, eval. unzip them.
        source_dirs = []
        for filename, _ in SOURCE_DATASETS:
            srcfile = os.path.join(srcdir, filename + '.zip')
            tempdir = unzip_dataset(srcfile, os.path.join(srcdir, filename))
            source_dirs.append(tempdir)

        # package monthly datasets for each month
        for month in range(0, 12):
            ziproot = create_target_dir(dest, '1km', month)
            convert(srcdir, ziproot, month+1)
            gen_metadatajson(JSON_TEMPLATE, ziproot, MONTH_LIST[month])
            zipbccvldataset(ziproot, dest)
    finally:
        # cleanup temp location
        if os.path.exists(ziproot):
            shutil.rmtree(ziproot)
        for tempdir in source_dirs:
            shutil.rmtree(tempdir)

if __name__ == "__main__":
    main(sys.argv)

