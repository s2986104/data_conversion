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


def parse_zip_filename(srcfile):
    """
    srcfile should be an absolute path name to a zip file in the source folder
    """
    basename, _ = os.path.splitext(os.path.basename(srcfile))
    basedir = os.path.basename(os.path.dirname(srcfile))
    res = '36s' if basedir == 'NaRCLIM_1km' else '9s'
    parts = basename.split('_')
    if parts[1] == 'baseline':
        # it's a current file
        gcm = 'current'
        emsc = '_'.join(parts[1:])
        year = int('2000')
    else:
        # it's future .. basedir is resolution
        _, year, gcm, rcm = parts
        emsc = 'SRES-A2'
        gcm = '{}-{}'.format(gcm, rcm)
    return gcm, emsc, year, res


def gdal_options(srcfile):
    """
    options to add metadata for the tiff file
    """
    gcm, _, year, _ = parse_zip_filename(srcfile)
    options = ['-of', 'GTiff', '-co', 'TILED=YES']
    options += ['-co', 'COMPRESS=DEFLATE', '-norat']
    options += ['-mo', 'year_range={}-{}'.format(year-10, year+9)]
    options += ['-mo', 'year={}'.format(year)]

    if gcm != 'current':
        options += ['-mo', 'emission_scenario={}'.format(emsc)]
        options += ['-mo', 'general_circulation_models={}'.format(gcm.upper())]
    return options


def get_layer_id(filename):
    fname = os.path.splitext(os.path.basename(filename))[0]
    _, _, layerid = fname.split('_')
    return 'bioclim_{}'.format(layerid)


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

            if not zipinfo.filename.endswith('tif'):
                continue

            layerid = get_layer_id(os.path.basename(zipinfo.filename))
            destfilename = '{}.tif'.format(layerid) 
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
            raise result.excption()


def create_target_dir(destdir, srcfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    
    gcm, emsc, year, resolution = parse_zip_filename(srcfile)
    if gcm == 'current':
        dirname = '_'.join(('narclim', 'current', emsc, resolution))
    else:
        dirname = '_'.join(('narclim', emsc, gcm, str(year), resolution))
    root = os.path.join(destdir, resolution, dirname)
    return ensure_directory(root)


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
        default='/mnt/workdir/work_narclim',
        help=('folder to store working files before moving to final '
              'destination')
    )
    parser.add_argument(
        '--resolution', action='store',
        choices=['36s', '9s'],
        help='only convert files at specified resolution'
    )    
    return parser.parse_args()


def main():
    opts = parse_args()
    src = os.path.abspath(opts.source)

    workdir = ensure_directory(opts.workdir)
    dest = ensure_directory(opts.destdir)

    resdir = 'NaRCLIM_1km' if opts.resolution == '36s' else 'NaRCLIM_9s'
    if os.path.isdir(src):
        srcfiles = sorted(glob.glob(os.path.join(src, resdir, '*.zip')))
    else:
        srcfiles = [src]

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
