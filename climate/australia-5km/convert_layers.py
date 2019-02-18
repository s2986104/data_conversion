#!/usr/bin/env python
from concurrent import futures
import glob
import os
import os.path
import subprocess
import sys
import tempfile
import zipfile
import argparse

from osgeo import gdal
import tqdm

from data_conversion.vocabs import VAR_DEFS, PREDICTORS


# map source file id's to our idea of RCP id's
EMSC_MAP = {
    'RCP3PD': 'RCP2.6',
    'RCP6': 'RCP6.0',
    'RCP45': 'RCP4.5',
    'RCP85': 'RCP8.5',
}


def parse_zip_filename(srcfile):
    """Parse filename of the format RCP85_ncar-pcm1_2015.zip to get emsc and gcm and year
    """
    # this should always parse source file ... we know the dest file anyway
    basename = os.path.basename(srcfile)
    basename, _ = os.path.splitext(basename)
    parts = basename.split('_')

    if parts[0] == 'current':
        emsc, gcm, year = parts[0], parts[0], '1976-2005'
    else:
        emsc, gcm, year = parts
    return EMSC_MAP.get(emsc, emsc), gcm, year


def gdal_options(srcfile):
    # options to add metadata for the tiff file
    emsc, gcm, year = parse_zip_filename(srcfile)

    options = ['-of', 'GTiff', '-co', 'TILED=YES']
    options += ['-co', 'COMPRESS=DEFLATE', '-norat']
    if emsc == 'current':
        years = [int(x) for x in year.split('-')]
        options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
        options += ['-mo', 'year={}'.format(int((years[1] - years[0] + 1 / 2) + years[0]))]
    else:
        year = int(year)
        years = [year - 4, year + 5]
        options += ['-mo', 'emission_scenario={}'.format(emsc)]
        options += ['-mo', 'general_circulation_models={}'.format(gcm.upper())]
        options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
        options += ['-mo', 'year={}'.format(year)]
    return options


def get_layer_id(filename):
    layerid = os.path.splitext(os.path.basename(filename))[0]
    return layerid


def run_gdal(cmd, infile, outfile, layerid):
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
        for key, value in VAR_DEFS[layerid].items():
            band.SetMetadataItem(key, value)
        # just for completeness
        band.SetUnitType(VAR_DEFS[layerid]['units'])
        # band.SetScale(1.0)
        # band.SetOffset(0.0)
        ds.FlushCache()
        # build command
        cmd = [
            'gdal_translate',
            '-of', 'GTiff',
            '-co', 'TILED=yes',
            '-co', 'COPY_SRC_OVERVIEWS=YES',
            '-co', 'COMPRESS=DEFLATE',
            '-co', 'PREDICTOR={}'.format(PREDICTORS[band.DataType]),
        ]
        # check rs
        if not ds.GetProjection():
            cmd.extend(['-a_srs', 'EPSG:4326'])
        # close dataset
        del band
        del ds
        # gdal_translate once more to cloud optimise geotiff
        cmd.extend([tfname, outfile])
        ret = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        ret.check_returncode()
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

            layerid = get_layer_id(os.path.basename(zipinfo.filename))
            destfilename = (
                os.path.basename(destdir) +
                '_' +
                layerid.replace('_', '-') +
                '.tif'
            )
            srcurl = '/vsizip/' + srcfile + '/' + zipinfo.filename
            gdaloptions = gdal_options(srcfile)
            # output file name
            destpath = os.path.join(destdir, destfilename)
            # run gdal translate
            cmd = ['gdal_translate']
            cmd.extend(gdaloptions)
            results.append(pool.submit(run_gdal, cmd, srcurl, destpath, layerid))

    for result in tqdm.tqdm(futures.as_completed(results), desc=os.path.basename(srcfile), total=len(results)):
        if result.exception():
            print("Job failed")
            raise result.excption()


def create_target_dir(destdir, srcfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    emsc, gcm, year = parse_zip_filename(srcfile)
    if emsc == 'current':
        dirname = 'current_{year}'.format(year=year)
    else:
        dirname = '{0}_{1}_{2}'.format(emsc, gcm, year).replace(' ', '')
    root = os.path.join(destdir, dirname)
    os.makedirs(root, exist_ok=True)
    return root


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('source', action='store',
                        help='source folder or source zip file.')
    parser.add_argument('destdir', action='store',
                        help='destination folder for converted tif files.')
    return parser.parse_args()


def main():
    opts = parse_args()
    srcfile = os.path.abspath(opts.source)
    if os.path.isdir(srcfile):
        srcfiles = sorted(glob.glob(os.path.join(srcfile, '*.zip')))
    else:
        srcfiles = [srcfile]
    dest = os.path.abspath(opts.destdir)
    # unpack contains one destination datasets
    for srcfile in tqdm.tqdm(srcfiles):
        targetdir = create_target_dir(dest, srcfile)
        convert(srcfile, targetdir)


if __name__ == "__main__":
    main(sys.argv)
