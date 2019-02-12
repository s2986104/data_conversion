#!/usr/bin/env python
from concurrent import futures
import glob
import os
import os.path
import subprocess
import sys
import tempfile
import zipfile

from osgeo import gdal
import tqdm


VAR_DEFS = {
    'bioclim_01': {
        'standard_name': 'bioclim_01',
        'long_name': 'Bioclim 01: Annual mean temperature',
        'units': 'degree_Celsius'
    },
    'bioclim_02': {
        'standard_name': 'bioclim_02',
        'long_name': 'Bioclim 02: Mean Diurnal Range (Mean of monthly (max temp - min temp))',
        'units': 'degree_Celsius',
    },
    'bioclim_03': {
        'standard_name': 'bioclim_03',
        'long_name': 'Bioclim 03: Isothermality (BIO2/BIO7) (* 100)',
        'units': '1',
    },
    'bioclim_04': {
        'standard_name': 'bioclim_04',
        'long_name': 'Bioclim 04: Temperature Seasonality (standard deviation *100)',
        'units': 'degree_Celsius',
    },
    'bioclim_05': {
        'standard_name': 'bioclim_05',
        'long_name': 'Bioclim 05: Max Temperature of Warmest Month',
        'units': 'degree_Celsius',
    },
    'bioclim_06': {
        'standard_name': 'bioclim_06',
        'long_name': 'Bioclim 06: Min Temperature of Coldest Month',
        'units': 'degree_Celsius',
    },
    'bioclim_07': {
        'standard_name': 'bioclim_07',
        'long_name': 'Bioclim 07: Temperature Annual Range (BIO5-BIO6)',
        'units': 'degree_Celsius',
    },
    'bioclim_08': {
        'standard_name': 'bioclim_08',
        'long_name': 'Bioclim 08: Mean Temperature of Wettest Quarter',
        'units': 'degree_Celsius',
    },
    'bioclim_09': {
        'standard_name': 'bioclim_09',
        'long_name': 'Bioclim 09: Mean Temperature of Driest Quarter',
        'units': 'degree_Celsius',
    },
    'bioclim_10': {
        'standard_name': 'bioclim_10',
        'long_name': 'Bioclim 10: Mean Temperature of Warmest Quarter',
        'units': 'degree_Celsius',
    },
    'bioclim_11': {
        'standard_name': 'bioclim_11',
        'long_name': 'Bioclim 11: Mean Temperature of Coldest Quarter',
        'units': 'degree_Celsius',
    },
    'bioclim_12': {
        'standard_name': 'bioclim_12',
        'long_name': 'Bioclim 12: Annual Precipitation',
        'units': 'mm ',
    },
    'bioclim_13': {
        'standard_name': 'bioclim_13',
        'long_name': 'Bioclim 13: Precipitation of Wettest Month',
        'units': 'mm',
    },
    'bioclim_14': {
        'standard_name': 'bioclim_14',
        'long_name': 'Bioclim 14: Precipitation of Driest Month',
        'units': 'mm',
    },
    'bioclim_15': {
        'standard_name': 'bioclim_15',
        'long_name': 'Bioclim 15: Precipitation Seasonality (Coefficient of Variation)',
        'units': '1',
    },
    'bioclim_16': {
        'standard_name': 'bioclim_16',
        'long_name': 'Bioclim 16: Precipitation of Wettest Quarter',
        'units': 'mm',
    },
    'bioclim_17': {
        'standard_name': 'bioclim_17',
        'long_name': 'Bioclim 17: Precipitation of Driest Quarter',
        'units': 'mm',
    },
    'bioclim_18': {
        'standard_name': 'bioclim_18',
        'long_name': 'Bioclim 18: Precipitation of Warmest Quarter',
        'units': 'mm',
    },
    'bioclim_19': {
        'standard_name': 'bioclim_19',
        'long_name': 'Bioclim 19: Precipitation of Coldest Quarter',
        'units': 'mm',
    },

}


LAYER_MD = {
    'bioclim_01.tif': {'var': 'bioclim_01', 'predictor': '3'},
    'bioclim_02.tif': {'var': 'bioclim_02', 'predictor': '3'},
    'bioclim_03.tif': {'var': 'bioclim_03', 'predictor': '3'},
    'bioclim_04.tif': {'var': 'bioclim_04', 'predictor': '3'},
    'bioclim_05.tif': {'var': 'bioclim_05', 'predictor': '3'},
    'bioclim_06.tif': {'var': 'bioclim_06', 'predictor': '3'},
    'bioclim_07.tif': {'var': 'bioclim_07', 'predictor': '3'},
    'bioclim_08.tif': {'var': 'bioclim_08', 'predictor': '3'},
    'bioclim_09.tif': {'var': 'bioclim_09', 'predictor': '3'},
    'bioclim_10.tif': {'var': 'bioclim_10', 'predictor': '3'},
    'bioclim_11.tif': {'var': 'bioclim_11', 'predictor': '3'},
    'bioclim_12.tif': {'var': 'bioclim_12', 'predictor': '3'},
    'bioclim_13.tif': {'var': 'bioclim_13', 'predictor': '3'},
    'bioclim_14.tif': {'var': 'bioclim_14', 'predictor': '3'},
    'bioclim_15.tif': {'var': 'bioclim_15', 'predictor': '3'},
    'bioclim_16.tif': {'var': 'bioclim_16', 'predictor': '3'},
    'bioclim_17.tif': {'var': 'bioclim_17', 'predictor': '3'},
    'bioclim_18.tif': {'var': 'bioclim_18', 'predictor': '3'},
    'bioclim_19.tif': {'var': 'bioclim_19', 'predictor': '3'},
}

# map source file id's to our idea of RCP id's
EMSC_MAP = {
    'RCP3PD': 'RCP2.6',
    'RCP6': 'RCP6.0',
    'RCP45': 'RCP4.5',
    'RCP85': 'RCP8.5',
}


def parse_filename(fname):
    """Parse filename of the format RCP85_ncar-pcm1_2015.zip to get emsc and gcm and year
    """
    basename = os.path.basename(fname)
    basename, _ = os.path.splitext(basename)
    emsc, gcms, year = basename.split('_')
    return EMSC_MAP.get(emsc, emsc), gcms, year


def gdal_options(srcurl, destfilename, destdir):
    # options to add metadata for the tiff file
    md = LAYER_MD.get(destfilename)
    if not md:
        raise Exception("layer {0} is missing metadata".format(destfilename))

    options = ['-of', 'GTiff', '-co', 'TILED=YES']
    options += ['-co', 'COMPRESS=DEFLATE']
    # PREDICTOR=2 sholud only be used with integer continous data
    # PREDICTOR=3 for floating point data
    options += ['-co', 'PREDICTOR={}'.format(md['predictor'])]
    if os.path.basename(destdir).startswith("current_"):
        _, year = os.path.basename(destdir).split('_')
        years = [int(x) for x in year.split('-')]
        options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
        options += ['-mo', 'year={}'.format(int((years[1] - years[0] + 1 / 2) + years[0]))]
    else:
        emsc, gcms, year = os.path.basename(destdir).split('_')
        year = int(year)
        years = [year - 4, year + 5]
        options += ['-mo', 'emission_scenario={}'.format(emsc)]
        options += ['-mo', 'general_circulation_models={}'.format(gcms.upper())]
        options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
        options += ['-mo', 'year={}'.format(year)]
    # set CRS
    options += ['-a_srs', 'EPSG:4326']
    return options


def run_gdal(cmd, infile, outfile, md):
    tf, tfname = tempfile.mkstemp(suffix='.tif')
    try:
        ret = subprocess.run(
            cmd + [infile, tfname],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        # raise an exception on error
        ret.check_returncode()
        # add band metadata
        ds = gdal.Open(tfname, gdal.GA_Update)
        band = ds.GetRasterBand(1)
        # ensure band stats
        band.ComputeStatistics(False)
        for key, value in VAR_DEFS[md['var']].items():
            band.SetMetadataItem(key, value)
        # just for completeness
        band.SetUnitType(VAR_DEFS[md['var']]['units'])
        # band.SetScale(1.0)
        # band.SetOffset(0.0)

        ds.FlushCache()
        # close dataset
        del ds
        # gdal_translate once more to cloud optimise geotiff
        ret = subprocess.run(
            [
                'gdal_translate',
                '-of', 'GTiff',
                '-co', 'TILED=YES',
                '-co', 'COPY_SRC_OVERVIEWS=YES',
                '-co', 'COMPRESS=DEFLATE',
                '-co', 'PREDICTOR={}'.format(md['predictor']),
                tfname,
                outfile,
            ],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except Exception as e:
        print('Error:', e)
    finally:
        os.remove(tfname)


def convert(srcfile, destdir):
    """convert .asc.gz files in folder to .tif in dest
    """

    pool = futures.ProcessPoolExecutor()
    results = []
    with zipfile.ZipFile(srcfile) as srczip:
        for zipinfo in tqdm.tqdm(srczip.filelist, desc="build jobs"):
            if zipinfo.is_dir():
                # skip dir entries
                continue
            if not zipinfo.filename.endswith('.asc'):
                # skip non .asc files
                continue
            destfilename = os.path.basename(zipinfo.filename)[:-len('.asc')] + '.tif'
            srcurl = '/vsizip/' + srcfile + '/' + zipinfo.filename
            gdaloptions = gdal_options(srcurl, destfilename, destdir)
            # output file name
            destpath = os.path.join(destdir, destfilename)
            # run gdal translate
            cmd = ['gdal_translate']
            cmd.extend(gdaloptions)
            results.append(pool.submit(run_gdal, cmd, srcurl, destpath, LAYER_MD.get(destfilename)))

    for result in tqdm.tqdm(futures.as_completed(results), desc=os.path.basename(srcfile), total=len(results)):
        if result.exception():
            print("Job failed")
            raise result.excption()


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
    os.makedirs(root, exist_ok=True)
    return root


def main(argv):
    if len(argv) != 3:
        print("Usage: {0} <srczip> <destdir>".format(argv[0]))
        print("       if <srczip> is a directory all zip files within will be converted.")
        sys.exit(1)
    srcfile = os.path.abspath(argv[1])
    if os.path.isdir(srcfile):
        srcfiles = sorted(glob.glob(os.path.join(srcfile, '*.zip')))
    else:
        srcfiles = [srcfile]
    dest = os.path.abspath(argv[2])
    # unpack contains one destination datasets
    for srcfile in tqdm.tqdm(srcfiles):
        targetdir = create_target_dir(dest, srcfile)
        convert(srcfile, targetdir)


if __name__ == "__main__":
    main(sys.argv)
