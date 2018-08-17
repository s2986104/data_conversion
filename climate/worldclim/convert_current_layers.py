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
import time
import argparse

TMPDIR = os.getenv("BCCVL_TMP", "/mnt/workdir/")

JSON_TEMPLATE = 'worldclim.template.json'
TITLE_TEMPLATE = u'WorldClim Current Conditions (1950-2000) at {}'

FILE_MAP = {
    'alt':    'altitude',
    'prec_1': 'prec_01',
    'prec_2': 'prec_02',
    'prec_3': 'prec_03',
    'prec_4': 'prec_04',
    'prec_5': 'prec_05',
    'prec_6': 'prec_06',
    'prec_7': 'prec_07',
    'prec_8': 'prec_08',
    'prec_9': 'prec_09',
    'prec_10': 'prec_10',
    'prec_11': 'prec_11',
    'prec_12': 'prec_12',
    'tmax_1': 'tmax_01',
    'tmax_2': 'tmax_02',
    'tmax_3': 'tmax_03',
    'tmax_4': 'tmax_04',
    'tmax_5': 'tmax_05',
    'tmax_6': 'tmax_06',
    'tmax_7': 'tmax_07',
    'tmax_8': 'tmax_08',
    'tmax_9': 'tmax_09',
    'tmax_10': 'tmax_10',
    'tmax_11': 'tmax_11',
    'tmax_12': 'tmax_12',
    'tmean_1': 'tmean_01',
    'tmean_2': 'tmean_02',
    'tmean_3': 'tmean_03',
    'tmean_4': 'tmean_04',
    'tmean_5': 'tmean_05',
    'tmean_6': 'tmean_06',
    'tmean_7': 'tmean_07',
    'tmean_8': 'tmean_08',
    'tmean_9': 'tmean_09',
    'tmean_10': 'tmean_10',
    'tmean_11': 'tmean_11',
    'tmean_12': 'tmean_12',
    'tmin_1': 'tmin_01',
    'tmin_2': 'tmin_02',
    'tmin_3': 'tmin_03',
    'tmin_4': 'tmin_04',
    'tmin_5': 'tmin_05',
    'tmin_6': 'tmin_06',
    'tmin_7': 'tmin_07',
    'tmin_8': 'tmin_08',
    'tmin_9': 'tmin_09',
    'tmin_10': 'tmin_10',
    'tmin_11': 'tmin_11',
    'tmin_12': 'tmin_12',
    'bio_1': 'bioclim_01',
    'bio_2': 'bioclim_02',
    'bio_3': 'bioclim_03',
    'bio_4': 'bioclim_04',
    'bio_5': 'bioclim_05',
    'bio_6': 'bioclim_06',
    'bio_7': 'bioclim_07',
    'bio_8': 'bioclim_08',
    'bio_9': 'bioclim_09',
    'bio_10': 'bioclim_10',
    'bio_11': 'bioclim_11',
    'bio_12': 'bioclim_12',
    'bio_13': 'bioclim_13',
    'bio_14': 'bioclim_14',
    'bio_15': 'bioclim_15',
    'bio_16': 'bioclim_16',
    'bio_17': 'bioclim_17',
    'bio_18': 'bioclim_18',
    'bio_19': 'bioclim_19',
}

RESOLUTION_MAP = {
    '30s': '30 arcsec',
    '2-5m': '2.5 arcmin',
    '5m': '5 arcmin',
    '10m': '10 arcmin',
}

LAYER_TYPE_MAP = {
   'alt': 'alt',
   'tmax': 'tmax',
   'tmin': 'tmin',
   'tmean': 'tmean',
   'prec': 'prec',
   'bio': 'bioclim',
}


# Metadata for layers: variable/layer name, description, unit, scale factor
LAYER_MD = {
    'altitude.tif': ('Altitude', 'altitude (elevation above sea level)', 'metre', None),
    'bioclim_01.tif': ('B01', 'annual mean temperature', 'degree_Celsius', 0.1),
    'bioclim_02.tif': ('B02', 'mean diurnal temperature range', 'degree_Celsius', 0.1),
    'bioclim_03.tif': ('B03', 'isothermality', None, 0.01),
    'bioclim_04.tif': ('B04', 'temperature seasonality', 'degree_Celsius', 0.001),
    'bioclim_05.tif': ('B05', 'max temperature of warmest week', 'degree_Celsius', 0.1),
    'bioclim_06.tif': ('B06', 'min temperature of coldest week', 'degree_Celsius', 0.1),
    'bioclim_07.tif': ('B07', 'temperature annual range', 'degree_Celsius', 0.1),
    'bioclim_08.tif': ('B08', 'mean temperature of wettest quarter', 'degree_Celsius', 0.1),
    'bioclim_09.tif': ('B09', 'mean temperature of driest quarter', 'degree_Celsius', 0.1),
    'bioclim_10.tif': ('B10', 'mean temperature of warmest quarter', 'degree_Celsius', 0.1),
    'bioclim_11.tif': ('B11', 'mean temperature of coldest quarter', 'degree_Celsius', 0.1),
    'bioclim_12.tif': ('B12', 'annual precipitation', 'millimeter', None),
    'bioclim_13.tif': ('B13', 'precipitation of wettest week', 'millimeter', None),
    'bioclim_14.tif': ('B14', 'precipitation of driest week', 'millimeter', None),
    'bioclim_15.tif': ('B15', 'precipitation seasonality', 'millimeter', 0.01),
    'bioclim_16.tif': ('B16', 'precipitation of wettest quarter', 'millimeter', None),
    'bioclim_17.tif': ('B17', 'precipitation of driest quarter', 'millimeter', None),
    'bioclim_18.tif': ('B18', 'precipitation of warmest quarter', 'millimeter', None),
    'bioclim_19.tif': ('B19', 'precipitation of coldest quarter', 'millimeter', None),
    'prec_01.tif': ('PR1', 'average monthly precipitation (Jan)', 'millimeter', None),
    'prec_02.tif': ('PR2', 'average monthly precipitation (Feb)', 'millimeter', None),
    'prec_03.tif': ('PR3', 'average monthly precipitation (Mar)', 'millimeter', None),
    'prec_04.tif': ('PR4', 'average monthly precipitation (Apr)', 'millimeter', None),
    'prec_05.tif': ('PR5', 'average monthly precipitation (May)', 'millimeter', None),
    'prec_06.tif': ('PR6', 'average monthly precipitation (Jun)', 'millimeter', None),
    'prec_07.tif': ('PR7', 'average monthly precipitation (Jul)', 'millimeter', None),
    'prec_08.tif': ('PR8', 'average monthly precipitation (Aug)', 'millimeter', None),
    'prec_09.tif': ('PR9', 'average monthly precipitation (Sep)', 'millimeter', None),
    'prec_10.tif': ('PR10', 'average monthly precipitation (Oct)', 'millimeter', None),
    'prec_11.tif': ('PR11', 'average monthly precipitation (Nov)', 'millimeter', None),
    'prec_12.tif': ('PR12', 'average monthly precipitation (Dec)', 'millimeter', None),
    'tmax_01.tif': ('TX1', 'average monthly maximum temperature (Jan)', 'degree_Celsius', 0.1),
    'tmax_02.tif': ('TX2', 'average monthly maximum temperature (Feb)', 'degree_Celsius', 0.1),
    'tmax_03.tif': ('TX3', 'average monthly maximum temperature (Mar)', 'degree_Celsius', 0.1),
    'tmax_04.tif': ('TX4', 'average monthly maximum temperature (Apr)', 'degree_Celsius', 0.1),
    'tmax_05.tif': ('TX5', 'average monthly maximum temperature (May)', 'degree_Celsius', 0.1),
    'tmax_06.tif': ('TX6', 'average monthly maximum temperature (Jun)', 'degree_Celsius', 0.1),
    'tmax_07.tif': ('TX7', 'average monthly maximum temperature (Jul)', 'degree_Celsius', 0.1),
    'tmax_08.tif': ('TX8', 'average monthly maximum temperature (Aug)', 'degree_Celsius', 0.1),
    'tmax_09.tif': ('TX9', 'average monthly maximum temperature (Sep)', 'degree_Celsius', 0.1),
    'tmax_10.tif': ('TX10', 'average monthly maximum temperature (Oct)', 'degree_Celsius', 0.1),
    'tmax_11.tif': ('TX11', 'average monthly maximum temperature (Nov)', 'degree_Celsius', 0.1),
    'tmax_12.tif': ('TX12', 'average monthly maximum temperature (Dec)', 'degree_Celsius', 0.1),
    'tmin_01.tif': ('TN1', 'average monthly minimum temperature (Jan)', 'degree_Celsius', 0.1),
    'tmin_02.tif': ('TN2', 'average monthly minimum temperature (Feb)', 'degree_Celsius', 0.1),
    'tmin_03.tif': ('TN3', 'average monthly minimum temperature (Mar)', 'degree_Celsius', 0.1),
    'tmin_04.tif': ('TN4', 'average monthly minimum temperature (Apr)', 'degree_Celsius', 0.1),
    'tmin_05.tif': ('TN5', 'average monthly minimum temperature (May)', 'degree_Celsius', 0.1),
    'tmin_06.tif': ('TN6', 'average monthly minimum temperature (Jun)', 'degree_Celsius', 0.1),
    'tmin_07.tif': ('TN7', 'average monthly minimum temperature (Jul)', 'degree_Celsius', 0.1),
    'tmin_08.tif': ('TN8', 'average monthly minimum temperature (Aug)', 'degree_Celsius', 0.1),
    'tmin_09.tif': ('TN9', 'average monthly minimum temperature (Sep)', 'degree_Celsius', 0.1),
    'tmin_10.tif': ('TN10', 'average monthly minimum temperature (Oct)', 'degree_Celsius', 0.1),
    'tmin_11.tif': ('TN11', 'average monthly minimum temperature (Nov)', 'degree_Celsius', 0.1),
    'tmin_12.tif': ('TN12', 'average monthly minimum temperature (Dec)', 'degree_Celsius', 0.1),
    'tmean_01.tif': ('TM1', 'average monthly mean temperature (Jan)', 'degree_Celsius', 0.1),
    'tmean_02.tif': ('TM2', 'average monthly mean temperature (Feb)', 'degree_Celsius', 0.1),
    'tmean_03.tif': ('TM3', 'average monthly mean temperature (Mar)', 'degree_Celsius', 0.1),
    'tmean_04.tif': ('TM4', 'average monthly mean temperature (Apr)', 'degree_Celsius', 0.1),
    'tmean_05.tif': ('TM5', 'average monthly mean temperature (May)', 'degree_Celsius', 0.1),
    'tmean_06.tif': ('TM6', 'average monthly mean temperature (Jun)', 'degree_Celsius', 0.1),
    'tmean_07.tif': ('TM7', 'average monthly mean temperature (Jul)', 'degree_Celsius', 0.1),
    'tmean_08.tif': ('TM8', 'average monthly mean temperature (Aug)', 'degree_Celsius', 0.1),
    'tmean_09.tif': ('TM9', 'average monthly mean temperature (Sep)', 'degree_Celsius', 0.1),
    'tmean_10.tif': ('TM10', 'average monthly mean temperature (Oct)', 'degree_Celsius', 0.1),
    'tmean_11.tif': ('TM11', 'average monthly mean temperature (Nov)', 'degree_Celsius', 0.1),
    'tmean_12.tif': ('TM12', 'average monthly mean temperature (Dec)', 'degree_Celsius', 0.1)
}



def unpack(zipname, path):
    """unpack zipfile to path
    """
    tries = 0
    # Make sure file is online
    while True:
        try:
            tries += 1
            zipf = zipfile.ZipFile(zipname, 'r')
            print "File {0} is online".format(zipname)
            break
        except Exception as e:
            if tries > 10:
                print "Fail to make file {0} online!!".format(zipname)
                raise Exception("Fail to make file {0} online!!".format(zipname))
            print "Waiting for file {0} to be online ...".format(zipname)
            time.sleep(60)
    zipf.extractall(path)

def unzip_dataset(dsfile):
    """unzip source dataset and return unzip location
    """
    tmpdir = tempfile.mkdtemp(dir=TMPDIR)
    try:
        unpack(dsfile, tmpdir)
    except:
        shutil.rmtree(tmpdir)
        raise
    return tmpdir

def metadata_options(filename, destdir):
    # options to add metadata for the tiff file
    md = LAYER_MD.get(filename)
    if not md:
        raise Exception("layer {0} is missing metadata".format(filename))

    options = '-of GTiff -co "COMPRESS=LZW" -co "TILED=YES"'
    year = '1950-2000'
    emsc = gcms = None

    if emsc:
        emsc = emsc.replace('RCP', 'RCP ')
        options += ' -mo "emission_scenario={}"'.format(emsc)
    if gcms:
        options += ' -mo "general_circulation_models={}"'.format(gcms.upper())
    if year:
        options += ' -mo "year={}"'.format(year)
    if md[0]:
        options += ' -mo "standard_name={}"'.format(md[0])
    if md[1]:
        options += ' -mo "long_name={}"'.format(md[1])
    if md[2]:
        options += ' -mo "unit={}"'.format(md[2])
    return options


def convert(filename, folder, dest):
    """convert .asc.gz files in folder to .tif in dest
    """
    # parse info from filename
    base = os.path.basename(filename)
    m = re.match(r'(\w*)_([\w-]*)_(\d*)', base)
    layer = m.group(1)
    for srcfile in glob.glob(os.path.join(folder, '{0}/{0}*'.format(layer))):
        # map filenames to common layer file names
        basename = FILE_MAP[os.path.basename(srcfile)] + '.tif'
        options = metadata_options(basename, dest)     
        # dest filename = dirname_variablename.tif
        dfilename = os.path.basename(dest) + '_' + LAYER_MD.get(basename)[0] + '.tif'
        destfile = os.path.join(dest, dfilename) 
        ret = os.system('gdal_translate {2} {0} {1}'.format(srcfile, 
                                                            destfile, 
                                                            options))

        if ret != 0:
            raise Exception("can't gdal_translate {0} ({1})".format(srcfile,
                                                                    ret))
        # Add factor and offset metedata to the band data
        scale = LAYER_MD.get(basename)[3]
        if scale is not None:
            ret = os.system('gdal_edit.py -scale {0} -offset {1} {2}'.format(scale, 0, destfile))
            if ret != 0:
                raise Exception("can't gdal_edit.py {0} ({1})".format(destfile, scale))
        else:
            # delete .aux.xml files as they only contain histogram data
            if os.path.exists(destfile + '.aux.xml'):
                os.remove(destfile + '.aux.xml')


def create_target_dir(destdir, destfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    root = os.path.join(destdir, destfile)
    # TODO: make sure there is no stray "root" folder left, shou probably delete it here if it alreayd exists
    os.mkdir(root)
    return root


def main(argv):
    ziproot = None

    parser = argparse.ArgumentParser(description='Convert WorldClim current datasets')
    parser.add_argument('srcdir', type=str, help='source directory')
    parser.add_argument('destdir', type=str, help='output directory')
    parser.add_argument('--dstype', type=str, choices=LAYER_TYPE_MAP.keys(), help='dataset type')
    params = vars(parser.parse_args(argv[1:]))
    src = params.get('srcdir')
    dest = params.get('destdir')
    dstypes = [params.get('dstype')] if params.get('dstype') is not None else LAYER_TYPE_MAP.keys()

    # fail if destination exists but is not a directory
    if os.path.exists(
            os.path.abspath(dest)) and not os.path.isdir(
            os.path.abspath(dest)):
        print "Path {} exists and is not a directory.".format(os.path.abspath(dest))
        sys.exit(os.EX_IOERR)

    # try to create destination if it doesn't exist
    if not os.path.isdir(os.path.abspath(dest)):
        try:
            os.makedirs(os.path.abspath(dest))
        except Exception as e:
            print "Failed to create directory at {}.".format(os.path.abspath(dest))
            sys.exit(os.EX_IOERR)

    for res in sorted(RESOLUTION_MAP.keys()):
        # sorting isn't important, it just forces it to
        # hit the smallest dataset first for testing
        for prefix in LAYER_TYPE_MAP.keys():
            if prefix not in dstypes:
                continue
            destfile = 'worldclim_{}_{}'.format(res, LAYER_TYPE_MAP[prefix])
            ziproot = create_target_dir(dest, destfile)
            for srcfile in glob.glob(
                    os.path.join(src, '{}_{}_*'.format(prefix, res))):
                srctmpdir = unzip_dataset(srcfile)
                convert(srcfile, srctmpdir, ziproot)
                if srctmpdir:
                    shutil.rmtree(srctmpdir)

if __name__ == "__main__":
    main(sys.argv)
