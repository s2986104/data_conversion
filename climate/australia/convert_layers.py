#!/usr/bin/env python
import argparse
from collections import namedtuple
from concurrent import futures
import glob
import os
import os.path
import shutil
import tempfile
import zipfile

from osgeo import gdal
import tqdm

from data_conversion.vocabs import VAR_DEFS, PREDICTORS
from data_conversion.utils import ensure_directory, move_files, retry_run_cmd

# map source file id's to our idea of RCP id's
EMSC_MAP = {
    'RCP3PD': 'RCP2.6',
    'RCP6': 'RCP6.0',
    'RCP45': 'RCP4.5',
    'RCP85': 'RCP8.5',
}


PARSE_RESULT = namedtuple(
    'PARSE_RESULT',
    ['resolution', 'emsc', 'gcm', 'year']
)
def parse_zip_filename(srcfile):
    """
    Parse filename of the format 1km/RCP85_ncar-pcm1_2015.zip to get emsc and
    gcm and year and resolution
    """
    # this should always parse source file ... we know the dest file anyway
    resolution = os.path.basename(os.path.dirname(srcfile))

    basename = os.path.basename(srcfile)
    basename, _ = os.path.splitext(basename)
    parts = basename.split('_')

    if parts[0].startswith('current'):
        emsc, gcm, year = 'current', 'current', '1976-2005'
    else:
        emsc, gcm, year = parts
    return PARSE_RESULT(
        resolution, EMSC_MAP.get(emsc, emsc), gcm, year
    )


def gdal_options(srcfile):
    """
    options to add metadata for the tiff file
    """
    _, emsc, gcm, year = parse_zip_filename(srcfile)

    options = ['-of', 'GTiff', '-co', 'TILED=YES']
    options += ['-co', 'COMPRESS=DEFLATE', '-norat']
    if emsc == 'current':
        years = [int(x) for x in year.split('-')]
        options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
        options += [
            '-mo', 'year={}'.format(
                int(((years[1] - years[0] - 1) / 2) + years[0])
            )
        ]
    else:
        year = int(year)
        years = [year - 4, year + 5]
        options += ['-mo', 'emission_scenario={}'.format(emsc)]
        options += ['-mo', 'general_circulation_models={}'.format(gcm.upper())]
        options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
        options += ['-mo', 'year={}'.format(year)]
    return options


# This get's layerid from filename within zip file 
def get_layer_id(filename):
    layerid = os.path.splitext(os.path.basename(filename))[0]
    return layerid


def run_gdal(cmd, infile, outfile, layerid):
    _, tfname = tempfile.mkstemp(suffix='.tif')
    try:
        retry_run_cmd(cmd + [infile, tfname])
        # add band metadata
        # this is our temporary geo tiff, we should be able to open that
        # without problems
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
        retry_run_cmd(cmd)
    except Exception as e:
        print('Error:', e)
        raise e
    finally:
        os.remove(tfname)


def convert(srcfile, destdir):
    """convert .asc.gz files in folder to .tif in dest
    """

    pool = futures.ProcessPoolExecutor(3)
    results = []
    with zipfile.ZipFile(srcfile) as srczip:
        for zipinfo in tqdm.tqdm(srczip.filelist, desc="build jobs"):
            if zipinfo.is_dir():
                # skip dir entries
                continue
            if not zipinfo.filename.endswith('.asc'):
                # skip non .asc files
                continue

            layerid = get_layer_id(zipinfo.filename)
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
            results.append(
                pool.submit(run_gdal, cmd, srcurl, destpath, layerid)
            )

    for result in tqdm.tqdm(futures.as_completed(results),
                            desc=os.path.basename(srcfile),
                            total=len(results)):
        if result.exception():
            print("Job failed")
            raise result.exception()


def create_target_dir(destdir, srcfile, check=False):
    """create zip folder structure in tmp location.
    return root folder
    """
    res, emsc, gcm, year = parse_zip_filename(srcfile)
    if emsc == 'current':
        dirname = 'current_{year}'.format(year=year)
    else:
        dirname = '{0}_{1}_{2}'.format(emsc, gcm, year).replace(' ', '')
    root = os.path.join(destdir, res, dirname)
    if check:
        return os.path.exists(root)
    else:
        os.makedirs(root, exist_ok=True)
    return root


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'source', action='store',
        help='source folder or source zip file.'
    )
    parser.add_argument(
        'destdir', action='store',
        help='destination folder for converted tif files.'
    )
    parser.add_argument(
        '--workdir', action='store',
        default='/mnt/workdir/australia_work',
        help=('folder to store working files before moving to final '
              'destination')
    )
    parser.add_argument(
        '--skipexisting', action='store_true',
        help='Skip files for which destination dir exists. (no checks done)'
    )
    return parser.parse_args()


def main():
    opts = parse_args()
    srcfile = os.path.abspath(opts.source)
    if os.path.isdir(srcfile):
        srcfiles = sorted(glob.glob(os.path.join(srcfile, '**', '*.zip'), recursive=True))
    else:
        srcfiles = [srcfile]

    workdir = ensure_directory(opts.workdir)
    dest = ensure_directory(opts.destdir)
    # unpack contains one destination datasets
    for srcfile in tqdm.tqdm(srcfiles):
        target_work_dir = create_target_dir(workdir, srcfile)
        try:
            if opts.skipexisting and create_target_dir(dest, srcfile, check=True):
                tqdm.tqdm.write('Skip {}'.format(srcfile))
                continue
            # convert files into workdir
            convert(srcfile, target_work_dir)
            # move results to dest
            target_dir = create_target_dir(dest, srcfile)
            move_files(target_work_dir, target_dir)
        finally:
            # cleanup
            shutil.rmtree(target_work_dir)


if __name__ == "__main__":
    main()
