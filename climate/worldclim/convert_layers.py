import re
import sys
import glob
import json
import os
import os.path
import zipfile
import itertools
import tempfile
import shutil
import time
import argparse

TMPDIR = os.getenv("BCCVL_TMP", "/mnt/workdir/")

## Lookups for metadata construction
#  Details taken from:
#    http://www.worldclim.org/cmip5_10m
#    http://www.worldclim.org/formats

RESOLUTION_MAP = {
    '30s': 'Resolution30s',
    '2.5m': 'Resolution2_5m',
    '5m': 'Resolution5m',
    '10m': 'Resolution10m',
}

# global climate models (GCMs)
GCM_MAP = {
    'ac': 'ACCESS1-0',
    'bc': 'BCC-CSM1-1',
    'cc': 'CCSM4',
    'ce': 'CESM1-CAM5-1-FV2',
    'cn': 'CNRM-CM5',
    'gf': 'GFDL-CM3',
    'gd': 'GFDL-ESM2G',
    'gs': 'GISS-E2-R',
    'hd': 'HadGEM2-A0',
    'hg': 'HadGEM2-CC',
    'he': 'HadGEM2-ES',
    'in': 'INMCM4',
    'ip': 'IPSL-CM5A-LR',
    'mi': 'MIROC-ESM-CHEM',
    'mr': 'MIROC-ESM',
    'mc': 'MIROC5',
    'mp': 'MPI-ESM-LR',
    'mg': 'MRI-CGCM3',
    'no': 'NorESM1-M',
}

# representative concentration pathways (RCPs)
RCP_MAP = {
    '26': 'RCP3PD',
    '45': 'RCP4.5',
    '60': 'RCP6',
    '85': 'RCP8.5',
}

#VARIABLE_MAP = {
#    'tn': 'Monthly Average Minimum Temperature',
#    'tx': 'Monthly Average Maximum Temperature',
#    'pr': 'Monthly Total Precipitation',
#    'bi': 'Bioclim',
#}

LAYER_TYPE_MAP = {
   'tn': 'tmin',
   'tx': 'tmax',
   'pr': 'prec',
   'bi': 'bioclim',
}


YEAR_MAP = {
    '50': '2050',
    '70': '2070',
}

##

GEOTIFF_PATTERN = re.compile(
    r"""
    (?P<gcm>..)                 # GCM
    (?P<rcp>[0-9][0-9])         # RCP
    (?P<layer_type>..)          # Layer type
    (?P<year>[0-9][0-9])        # Year
    (?P<layer_num>[0-9][0-9]?)  # Layer no.
    """,
    re.VERBOSE
)



# Metadata for layers: variable/layer name, description, unit, scale factor
LAYER_MD = {
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
    'prec_01.tif': ('PR01', 'average monthly precipitation (Jan)', 'millimeter', None),
    'prec_02.tif': ('PR02', 'average monthly precipitation (Feb)', 'millimeter', None),
    'prec_03.tif': ('PR03', 'average monthly precipitation (Mar)', 'millimeter', None),
    'prec_04.tif': ('PR04', 'average monthly precipitation (Apr)', 'millimeter', None),
    'prec_05.tif': ('PR05', 'average monthly precipitation (May)', 'millimeter', None),
    'prec_06.tif': ('PR06', 'average monthly precipitation (Jun)', 'millimeter', None),
    'prec_07.tif': ('PR07', 'average monthly precipitation (Jul)', 'millimeter', None),
    'prec_08.tif': ('PR08', 'average monthly precipitation (Aug)', 'millimeter', None),
    'prec_09.tif': ('PR09', 'average monthly precipitation (Sep)', 'millimeter', None),
    'prec_10.tif': ('PR10', 'average monthly precipitation (Oct)', 'millimeter', None),
    'prec_11.tif': ('PR11', 'average monthly precipitation (Nov)', 'millimeter', None),
    'prec_12.tif': ('PR12', 'average monthly precipitation (Dec)', 'millimeter', None),
    'tmax_01.tif': ('TX01', 'average monthly maximum temperature (Jan)', 'degree_Celsius', 0.1),
    'tmax_02.tif': ('TX02', 'average monthly maximum temperature (Feb)', 'degree_Celsius', 0.1),
    'tmax_03.tif': ('TX03', 'average monthly maximum temperature (Mar)', 'degree_Celsius', 0.1),
    'tmax_04.tif': ('TX04', 'average monthly maximum temperature (Apr)', 'degree_Celsius', 0.1),
    'tmax_05.tif': ('TX05', 'average monthly maximum temperature (May)', 'degree_Celsius', 0.1),
    'tmax_06.tif': ('TX06', 'average monthly maximum temperature (Jun)', 'degree_Celsius', 0.1),
    'tmax_07.tif': ('TX07', 'average monthly maximum temperature (Jul)', 'degree_Celsius', 0.1),
    'tmax_08.tif': ('TX08', 'average monthly maximum temperature (Aug)', 'degree_Celsius', 0.1),
    'tmax_09.tif': ('TX09', 'average monthly maximum temperature (Sep)', 'degree_Celsius', 0.1),
    'tmax_10.tif': ('TX10', 'average monthly maximum temperature (Oct)', 'degree_Celsius', 0.1),
    'tmax_11.tif': ('TX11', 'average monthly maximum temperature (Nov)', 'degree_Celsius', 0.1),
    'tmax_12.tif': ('TX12', 'average monthly maximum temperature (Dec)', 'degree_Celsius', 0.1),
    'tmin_01.tif': ('TN01', 'average monthly minimum temperature (Jan)', 'degree_Celsius', 0.1),
    'tmin_02.tif': ('TN02', 'average monthly minimum temperature (Feb)', 'degree_Celsius', 0.1),
    'tmin_03.tif': ('TN03', 'average monthly minimum temperature (Mar)', 'degree_Celsius', 0.1),
    'tmin_04.tif': ('TN04', 'average monthly minimum temperature (Apr)', 'degree_Celsius', 0.1),
    'tmin_05.tif': ('TN05', 'average monthly minimum temperature (May)', 'degree_Celsius', 0.1),
    'tmin_06.tif': ('TN06', 'average monthly minimum temperature (Jun)', 'degree_Celsius', 0.1),
    'tmin_07.tif': ('TN07', 'average monthly minimum temperature (Jul)', 'degree_Celsius', 0.1),
    'tmin_08.tif': ('TN08', 'average monthly minimum temperature (Aug)', 'degree_Celsius', 0.1),
    'tmin_09.tif': ('TN09', 'average monthly minimum temperature (Sep)', 'degree_Celsius', 0.1),
    'tmin_10.tif': ('TN10', 'average monthly minimum temperature (Oct)', 'degree_Celsius', 0.1),
    'tmin_11.tif': ('TN11', 'average monthly minimum temperature (Nov)', 'degree_Celsius', 0.1),
    'tmin_12.tif': ('TN12', 'average monthly minimum temperature (Dec)', 'degree_Celsius', 0.1)
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


def convert_filename(filename):
    geotiff_info = GEOTIFF_PATTERN.match(filename).groupdict()
    return "{0}_{1:02d}.tif".format(LAYER_TYPE_MAP[geotiff_info['layer_type']], int(geotiff_info['layer_num']))


def potential_converts(source):
    potentials = itertools.product(
        GCM_MAP, RCP_MAP, YEAR_MAP, RESOLUTION_MAP, LAYER_TYPE_MAP
    )
    for gcm, rcp, year, res, ltype in potentials:
        source_filter = '{gcm}{rcp}{ltype}{year}.zip'.format(**locals())
        source_path = os.path.join(source, res, source_filter)
        source_files = glob.glob(source_path)
        if source_files:
            yield gcm, rcp, year, res, ltype, source_files


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
        emsc, gcms, year, res, dstype = os.path.basename(destdir).split('_')

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


def convert(srcfolder, dest):
    """convert .asc.gz files in folder to .tif in dest
    """

    for srcfile in glob.glob(os.path.join(srcfolder, '*.tif')):
        layer_index = convert_filename(os.path.basename(srcfile))
        options = metadata_options(layer_index, dest)     
        # dest filename = dirname_variablename.tif
        dfilename = os.path.basename(dest) + '_' + LAYER_MD.get(layer_index)[0] + '.tif'
        destfile = os.path.join(dest, dfilename) 
        ret = os.system('gdal_translate {2} {0} {1}'.format(srcfile, 
                                                            destfile, 
                                                            options))
        if ret != 0:
            raise Exception("can't gdal_translate {0} ({1})".format(srcfile,
                                                                    ret))
        # Add factor and offset metedata to the band data
        scale = LAYER_MD.get(layer_index)[3]
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


def create_target_dir(destdir, srcfile, rcp, gcm, year, res, lyr_type):
    """create zip folder structure in tmp location.
    return root folder
    """
    if os.path.basename(srcfile) == 'current.zip':
        dirname = 'current_{year}'.format(year='1976-2005')
    else:
        dirname = '{rcp}_{gcm}_{year}_{res}_{type}'.format(
            rcp = get_emsc_str(RCP_MAP[rcp]).replace(' ', ''),
            gcm = GCM_MAP[gcm],
            year = YEAR_MAP[year],
            res = res,
            type = LAYER_TYPE_MAP[lyr_type],
        )

    root = os.path.join(destdir, dirname)
    os.mkdir(root)
    return root

def main(argv):
    parser = argparse.ArgumentParser(description='Convert WorldClim future datasets')
    parser.add_argument('srcdir', type=str, help='source directory')
    parser.add_argument('destdir', type=str, help='output directory')
    parser.add_argument('--dstype', type=str, choices=LAYER_TYPE_MAP.keys(), help='dataset type')
    parser.add_argument('--gcm', type=str, choices=GCM_MAP.keys(), help='General Circulation Model')
    parser.add_argument('--rcp', type=str, choices=RCP_MAP.keys(), help='Representative Concentration Pathways')
    parser.add_argument('--year', type=str, choices=YEAR_MAP.keys(), help='year')
    parser.add_argument('--res', type=str, choices=RESOLUTION_MAP.keys(), help='resolution')
    params = vars(parser.parse_args(argv[1:]))
    src = params.get('srcdir')
    dest = params.get('destdir')
    dstypes = [params.get('dstype')] if params.get('dstype') is not None else LAYER_TYPE_MAP.keys()
    gcm_list = [params.get('gcm')] if params.get('gcm') is not None else GCM_MAP.keys()
    rcp_list = [params.get('rcp')] if params.get('rcp') is not None else RCP_MAP.keys()
    year_list = [params.get('year')] if params.get('year') is not None else YEAR_MAP.keys()
    res_list = [params.get('res')] if params.get('res') is not None else RESOLUTION_MAP.keys()

    # fail if destination exists but is not a directory
    if os.path.exists(os.path.abspath(dest)) and not os.path.isdir(os.path.abspath(dest)):
        print "Path {} exists and is not a directory.".format(os.path.abspath(dest))
        sys.exit(os.EX_IOERR)

    # try to create destination if it doesn't exist
    if not os.path.isdir(os.path.abspath(dest)):
        try:
            os.makedirs(os.path.abspath(dest))
        except Exception as e:
            print "Failed to create directory at {}.".format(os.path.abspath(dest))
            sys.exit(os.EX_IOERR)

    for gcm, rcp, year, res, layer, files in potential_converts(src):
        if layer not in dstypes or gcm not in gcm_list or rcp not in rcp_list or year not in year_list or res not in res_list:
            continue
        for srczipf in files:
            srctmpdir = unzip_dataset(srczipf)

            # Create a dest directory for the datasets
            ziproot = create_target_dir(dest, srczipf, rcp, gcm, year, res, layer)

            convert(srctmpdir, ziproot)

if __name__ == "__main__":
    main(sys.argv)
