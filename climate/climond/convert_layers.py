#!/usr/bin/env python
from concurrent import futures
import glob
import os
import os.path
import tempfile
import zipfile
import argparse
import shutil

from osgeo import gdal
import tqdm

from data_conversion.vocabs import VAR_DEFS, PREDICTORS
from data_conversion.utils import ensure_directory, move_files, retry_run_cmd

# map source file id's to our idea of RCP id's
EMSC_MAP = {
    'A1B': 'SRES-A1B',
    'A2': 'SRES-A2'
}


def parse_zip_filename(srcfile):
    """
    Parse filename of the format CLIMOND_A2_CSIRO-MK3.0.zip to get emsc and
    gcm and year
    """
    # this should always parse source file ... we know the dest file anyway
    basename = os.path.basename(srcfile)
    basename, _ = os.path.splitext(basename)
    parts = basename.split('_')

    if parts[1] == 'current':
        emsc, gcm = parts[1], parts[1]
    else:
        emsc, gcm = parts[1], part[2]
    return EMSC_MAP.get(emsc, emsc), gcm


def gdal_options(srcfile, year):
    """
    options to add metadata for the tiff file
    """
    emsc, gcm = parse_zip_filename(srcfile)

    options = ['-of', 'GTiff', '-co', 'TILED=YES']
    options += ['-co', 'COMPRESS=DEFLATE', '-norat']
    year = int(year)
    years = [year - 14, year + 15]
    options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
    options += ['-mo', 'year={}'.format(year)]

    if emsc != 'current':
        options += ['-mo', 'emission_scenario={}'.format(emsc)]
        options += ['-mo', 'general_circulation_models={}'.format(gcm.upper())]
    return options


def get_layer_id(filename):
    # current dataset filename has 2 parts only
    parts = os.path.splitext(os.path.basename(filename))[0].split('_')
    layerid = 'bioclim_{}'.format(part[1] if len(parts) == 2 else part[3])
    year = '1976' if len(parts) == 2 else part[4]
    return layerid, int(year)


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

    pool = futures.ProcessPoolExecutor()
    results = []
    with zipfile.ZipFile(srcfile) as srczip:
        for zipinfo in tqdm.tqdm(srczip.filelist, desc="build jobs"):
            if zipinfo.is_dir():
                # skip dir entries
                continue
            if not zipinfo.filename.endswith('.tif'):
                # skip non .asc files
                continue

            layerid, year = get_layer_id(os.path.basename(zipinfo.filename))
            destfilename = (
                os.path.basename(destdir) +
                '_' +
                layerid.replace('_', '-') +
                '.tif'
            )
            srcurl = '/vsizip/' + srcfile + '/' + zipinfo.filename
            gdaloptions = gdal_options(srcfile, year)
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
            raise result.excption()


def create_target_dir(destdir, srcfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    emsc, gcm= parse_zip_filename(srcfile)
    if emsc == 'current':
        dirname = 'current'
    else:
        dirname = '{0}_{1}'.format(emsc, gcm).replace(' ', '')
    root = os.path.join(destdir, dirname)
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
        default='/mnt/workdir/australia-5km_work',
        help=('folder to store working files before moving to final '
              'destination')
    )
    return parser.parse_args()


def main():
    opts = parse_args()
    srcfile = os.path.abspath(opts.source)
    if os.path.isdir(srcfile):
        srcfiles = sorted(glob.glob(os.path.join(srcfile, '*.zip')))
    else:
        srcfiles = [srcfile]

    workdir = ensure_directory(opts.workdir)
    dest = ensure_directory(opts.destdir)
    # unpack contains one destination datasets
    for srcfile in tqdm.tqdm(srcfiles):
        target_work_dir = create_target_dir(workdir, srcfile)
        try:
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
