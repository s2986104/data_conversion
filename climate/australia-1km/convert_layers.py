#!/usr/bin/env python
import os
import os.path
import zipfile
import glob
import tempfile
import shutil
import sys
import re
import time

LAYER_MD = {
    'bioclim_01.tif': ('B01', 'annual mean temperature', 'degree_Celsius', None),
    'bioclim_02.tif': ('B02', 'mean diurnal temperature range', 'degree_Celsius', None),
    'bioclim_03.tif': ('B03', 'isothermality', None, None),
    'bioclim_04.tif': ('B04', 'temperature seasonality', 'degree_Celsius', None),
    'bioclim_05.tif': ('B05', 'max temperature of warmest week', 'degree_Celsius', None),
    'bioclim_06.tif': ('B06', 'min temperature of coldest week', 'degree_Celsius', None),
    'bioclim_07.tif': ('B07', 'temperature annual range', 'degree_Celsius', None),
    'bioclim_08.tif': ('B08', 'mean temperature of wettest quarter', 'degree_Celsius', None),
    'bioclim_09.tif': ('B09', 'mean temperature of driest quarter', 'degree_Celsius', None),
    'bioclim_10.tif': ('B10', 'mean temperature of warmest quarter', 'degree_Celsius', None),
    'bioclim_11.tif': ('B11', 'mean temperature of coldest quarter', 'degree_Celsius', None),
    'bioclim_12.tif': ('B12', 'annual precipitation', 'millimeter', None),
    'bioclim_13.tif': ('B13', 'precipitation of wettest week', 'millimeter', None),
    'bioclim_14.tif': ('B14', 'precipitation of driest week', 'millimeter', None),
    'bioclim_15.tif': ('B15', 'precipitation seasonality', 'millimeter', None),
    'bioclim_16.tif': ('B16', 'precipitation of wettest quarter', 'millimeter', None),
    'bioclim_17.tif': ('B17', 'precipitation of driest quarter', 'millimeter', None),
    'bioclim_18.tif': ('B18', 'precipitation of warmest quarter', 'millimeter', None),
    'bioclim_19.tif': ('B19', 'precipitation of coldest quarter', 'millimeter', None)
}


def unpack(zipname, path):
    """unpack zipfile to path
    """
    tries = 0
    while True:
        try:
            tries += 1
            zipf = zipfile.ZipFile(zipname, 'r')
            zipf.extractall(path)
            print "File {0} is online".format(zipname)
            break
        except Exception as e:
            if tries > 10:
                print "Fail to make file {0} online!!".format(zipname)
                raise Exception("Error: File {0} is not online".format(zipname))
            print "Waiting for file {0} to be online ...".format(zipname)
            time.sleep(60)


def get_emsc_str(emsc):
    if emsc == 'RCP3PD':
	   return 'RCP 2.6'
    if emsc == 'RCP6':
	   return 'RCP 6.0'
    if emsc == 'RCP45':
	   return 'RCP 4.5'
    if emsc == 'RCP85':
	   return 'RCP 8.5'
    return emsc


def parse_filename(fname):
    """Parse filename of the format RCP85_ncar-pcm1_2015.zip to get emsc and gcm and year
    """
    basename = os.path.basename(fname)
    basename, _ = os.path.splitext(basename)
    emsc, gcms, year = basename.split('_')
    return get_emsc_str(emsc), gcms, year

def metadata_options(filename, destdir):
    # options to add metadata for the tiff file
    md = LAYER_MD.get(filename)
    if not md:
        raise Exception("layer {0} is missing metadata".format(filename))

    options = '-of GTiff -co "COMPRESS=LZW" -co "TILED=YES"'
    if os.path.basename(destdir).startswith("current_"):
        _, year =  os.path.basename(destdir).split('_')
        emsc = gcms = None
    else:
        emsc, gcms, year = os.path.basename(destdir).split('_')

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

def convert(folder, dest):
    """convert .asc.gz files in folder to .tif in dest
    """

    for srcfile in glob.glob(os.path.join(folder, '*/*.asc')):
        filename = os.path.basename(srcfile)[:-len('.asc')] + '.tif'
        options = metadata_options(filename, dest)     
        # dest filename = dirname_variablename.tif
        dfilename = os.path.basename(dest) + '_' + LAYER_MD.get(filename)[0] + '.tif'
        destfile = os.path.join(dest, dfilename) 
        ret = os.system('gdal_translate {2} {0} {1}'.format(srcfile, 
                                                            destfile, 
                                                            options))
        if ret != 0:
            raise Exception("can't gdal_translate {0} ({1})".format(srcfile,
                                                                    ret))
        # Add factor and offset metedata to the band data
        scale = LAYER_MD.get(filename)[3]
        if scale is not None:
            ret = os.system('gdal_edit.py -scale {0} -offset {1} {2}'.format(scale, 0, destfile))
            if ret != 0:
                raise Exception("can't gdal_edit.py {0} ({1})".format(destfile, scale))

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


def create_target_dir(destdir, srcfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    if os.path.basename(srcfile) == 'current.zip':
        dirname = 'current_{year}'.format(year='1976-2005')
    else:
        emsc, gcms, year = parse_filename(srcfile)
        dirname = '{0}_{1}_{2}'.format(emsc, gcms, year).replace(' ', '')
    root = os.path.join(destdir, dirname)
    os.mkdir(root)
    return root


def main(argv):
    ziproot = None
    srctmpdir = None

    try:
        if len(argv) != 3:
            print "Usage: {0} <srczip> <destdir>".format(argv[0])
            sys.exit(1)
        srcfile = argv[1]
        dest = argv[2]
        # TODO: check src exists and is zip?
        # TODO: check dest exists
        srctmpdir = unzip_dataset(srcfile)
        # unpack contains one destination datasets
        ziproot = create_target_dir(dest, srcfile)
        convert(srctmpdir, ziproot)
    finally:
        # cleanup temp location
        if srctmpdir:
            shutil.rmtree(srctmpdir)

if __name__ == "__main__":
    main(sys.argv)
