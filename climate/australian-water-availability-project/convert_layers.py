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
from datetime import datetime

LAYER_MD = {
    'mth_FWDis': ('mth_FWDis', 'monthly local discharge (tunoff+drainage) (average)', 'mm/d', None),
    'pcr_mth_FWDis': ('pcr_mth_FWDis', 'monthly local discharge (runoff+drainage) (percentile rank data)', '%', None),
    'ann_FWDis': ('ann_FWDis', 'annual local discharge (runoff+drainage) (average)', 'mm/d', None),
    'pcr_ann_FWDis': ('pcr_ann_FWDis', 'annual local discharge (runoff+drainage) (percentile rank data)', '%', None),
    'ann_FWE': ('ann_FWE', 'annual total evaporation (soil+vegetation) (average)', 'mm/d', None),
    'pcr_ann_FWE': ('pcr_ann_FWE', 'annual total evaporation (soil+vegetation) (percentile rank data)', '%', None),
    'ann_FWLch2': ('ann_FWLch2', 'annual deep drainage (average)', 'mm/d', None),
    'pcr_ann_FWLch2': ('pcr_ann_FWLch2', 'annual deep drainage (percentile rank data)', '%', None),
    'ann_FWPT': ('ann_FWPT', 'annual potential evaporation (average)', 'mm/d', None),
    'pcr_ann_FWPT': ('pcr_ann_FWPT', 'annual potential evaporation (percentile rank data)', '%', None),
    'ann_FWRun': ('ann_FWRun', 'annual surface runoff (average)', 'mm/d', None),
    'pcr_ann_FWRun': ('pcr_ann_FWRun', 'annual surface runoff (percentile rank data)', '%', None),
    'ann_FWSoil': ('ann_FWSoil', 'annual soil evaporation (average)', 'mm/d', None),
    'pcr_ann_FWSoil': ('pcr_ann_FWSoil', 'annual soil evaporation (percentile rank data)', '%', None),
    'ann_FWTra': ('ann_FWTra', 'annual total transpiration (average)', 'mm/d', None),
    'pcr_ann_FWTra': ('pcr_ann_FWTra', 'annual total transpiration (percentile rank data)', '%', None),
    'ann_FWWater': ('ann_FWWater', 'annual open water evaporation (average)', 'mm/d', None),
    'pcr_ann_FWWater': ('pcr_ann_FWWater', 'annual open water evaporation (percentile rank data)', '%', None),
    'ann_PhiE': ('ann_PhiE', 'annual daily latent heat flux (average)', 'W/m^2', None),
    'pcr_ann_PhiE': ('pcr_ann_PhiE', 'annual daily latent heat flux (percentile rank data)', '%', None),
    'ann_PhiH': ('ann_PhiH', 'annual daily sensible heat flux (average)', 'W/m^2', None),
    'pcr_ann_PhiH': ('pcr_ann_PhiH', 'annual daily sensible heat flux (percentile rank data)', '%', None),
    'ann_SolarMJ': ('ann_SolarMJ', 'annual incident solar radiation (average)', 'MJ/m^2/d', None),
    'pcr_ann_SolarMJ': ('pcr_ann_SolarMJ', 'annual incident solar radiation (percentile rank data)', '%', None),
    'ann_TempMax': ('ann_TempMax', 'annual daily maximum temperature (average)', 'degree_Celsius', None),
    'pcr_ann_TempMax': ('pcr_ann_TempMax', 'annual daily maximum temperature (percentile rank data)', '%', None),
    'ann_TempMin': ('ann_TempMin', 'annual daily minimum temperature (average)', 'degree_Celsius', None),
    'pcr_ann_TempMin': ('pcr_ann_TempMin', 'annual daily minimum temperature (percentile rank data)', '%', None),
    'ann_WRel1': ('ann_WRel1', 'annual relative soil moisture (upper layer)', None, None),
    'pcr_ann_WRel1': ('pcr_ann_WRel1', 'annual relative soil moisture (upper layer) (percentile rank data)', '%', None),
    'ann_WRel1End': ('ann_WRel1End', 'annual relative soil moisture (upper layer) at end of aggregation period', None, None),
    'pcr_ann_WRel1End': ('pcr_ann_WRel1End', 'annual relative soil moisture (upper layer) at end of aggregation period (percentile rank data)', '%', None),
    'ann_WRel2': ('ann_WRel2', 'annual relative soil moisture (lower layer)', None, None),
    'pcr_ann_WRel2': ('pcr_ann_WRel2', 'annual relative soil moisture (lower layer) (percentile rank data)', '%', None),
    'ann_WRel2End': ('ann_WRel2End', 'annual relative soil moisture (lower layer) at end of aggregation period', None, None),
    'pcr_ann_WRel2End': ('pcr_ann_WRel2End', 'annual relative soil moisture (lower layer) at end of aggregation period (percentile rank data)', '%', None),
    'ann_Precip': ('ann_Precip', 'annual precipitation (average)', 'mm/d', None),
    'pcr_ann_Precip': ('pcr_ann_Precip', 'annual relative precipitation (percentile rank data)', '%', None)
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


def get_md(filename):
    # Get the layer md
    nameparts = os.path.splitext(filename)[0].split('_')
    date = nameparts[-1]
    year = datetime.strptime(date, '%Y%m%d').year
    layer_name = '_'.join(nameparts[:-1])
    md = LAYER_MD.get(layer_name)
    return md, year


def metadata_options(md, year):
    # options to add metadata for the tiff file
    options = '-of GTiff -co "COMPRESS=LZW" -co "TILED=YES" -a_srs EPSG:4326'

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
    """convert .flt files in folder to .tif in dest
    """
    # only interested in annual data
    for srcfile in glob.glob(os.path.join(folder, '*/*/*/*ann*.flt')):
        filename = os.path.basename(srcfile)[:-len('.flt')] + '.tif'
        md, year = get_md(filename)
        if md is None:
            continue
        options = metadata_options(md, year)   
        # dest filename = dirname_variablename.tif
        dfilename = os.path.basename(dest) + '_' + filename
        destfile = os.path.join(dest, dfilename) 
        ret = os.system('gdal_translate {2} {0} {1}'.format(srcfile, 
                                                            destfile, 
                                                            options))
        if ret != 0:
            raise Exception("can't gdal_translate {0} ({1})".format(srcfile,
                                                                    ret))
        # Add factor and offset metedata to the band data
        scale = md[3]
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
    # Extract the date from source file name i.e. 19500101_19501231.FWDis.run26j.flt.zip
    filename = os.path.basename(srcfile)
    date = filename.split('.')[0].split('_')[1]
    root = os.path.join(destdir, 'awap_ann_{}'.format(date))
    if not os.path.isdir(root):
        os.mkdir(root)
    return root


def main(argv):
    ziproot = None
    srctmpdir = None

    if len(argv) != 3:
        print "Usage: {0} <srczip> <destdir>".format(argv[0])
        sys.exit(1)
    srcdir = argv[1]
    dest = argv[2]
    for srcfile in glob.glob(os.path.join(srcdir, '*/*.zip')):
        try:
            # TODO: check src exists and is zip?
            # TODO: check dest exists
            srctmpdir = unzip_dataset(srcfile)
            # unpack contains one destination datasets
            ziproot = create_target_dir(dest, srcfile)
            convert(srctmpdir, ziproot)
            if srctmpdir:
                shutil.rmtree(srctmpdir)
        except Exception as e:
            # cleanup temp location
            if srctmpdir:
                shutil.rmtree(srctmpdir)

            print("Error: Cannot convert {}".format(srcfile))

if __name__ == "__main__":
    main(sys.argv)
