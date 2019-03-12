#!/usr/bin/env python
import argparse
from concurrent import futures
import glob
import os
import os.path
import tempfile
import zipfile
import shutil

from osgeo import gdal
import tqdm

from data_conversion.vocabs import VAR_DEFS, PREDICTORS
from data_conversion.utils import ensure_directory, move_files, retry_run_cmd


LAYERINFO = {
    # NVIS Australian vegetation group
    # source directory file, ('source fragment', year, dest filename, layerid)
    'GRID_NVIS4_2_AUST_EXT_MVG': ('aus4_2e_mvg', 2016, 'nvis_present_vegetation_groups.tif', 'AMVG'),
    'GRID_NVIS4_2_AUST_PRE_MVG': ('aus4_2p_mvg', 2016, 'nvis_pre-1750_vegetation_groups.tif', 'AMVG-1750')
}


def gdal_options(srcfile, year):
    # options to add metadata for the tiff file

    options = ['-of', 'GTiff', '-co', 'TILED=YES']
    options += ['-co', 'COMPRESS=DEFLATE', '--config', 'GDAL_PAM_MODE', 'PAM']
    options += ['-mo', 'year_range={}-{}'.format(year, year)]
    options += ['-mo', 'year={}'.format(year)]
    return options


def get_layer_id(filename):
    fname = os.path.splitext(os.path.basename(filename))[0]
    return LAYERINFO[fname]


def run_gdal(cmd, infile, outfile, layerid):
    tf, tfname = tempfile.mkstemp(suffix='.tif')
    try:
        retry_run_cmd(cmd + [infile, tfname])
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
            '--config', 'GDAL_PAM_MODE', 'PAM'
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
    finally:
        os.remove(tfname)


def convert(srcfile, destdir):
    """convert .asc.gz files in folder to .tif in dest
    """

    pool = futures.ProcessPoolExecutor()
    results = []
    with zipfile.ZipFile(srcfile) as srczip:
        srcfrag, year, destfname, layerid = get_layer_id(os.path.basename(srcfile))
        esrifname = '/'.join([fname, srcfrag, 'w001001.adf'])
        for zipinfo in tqdm.tqdm(srczip.filelist, desc="build jobs"):
            if zipinfo.is_dir():
                # skip dir entries
                continue
            if zipinfo.filename != esrifname:
                # skip non .asc files
                continue

            destfilename = srcfrag + '.tif'
            srcurl = '/vsizip/' + srcfile + '/' + zipinfo.filename
            gdaloptions = gdal_options(srcfile, year)
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
    basename = os.path.basename(srcfile)
    basename, _ = os.path.splitext(basename)
    root = os.path.join(destdir, basename)
    os.makedirs(root, exist_ok=True)
    return root


def parse_args():
    """
    parse cli
    """
    parser = argparse.ArgumentParser(
        description='Convert National SoilGrids datasets'
    )
    parser.add_argument(
        'srcdir', action='store',
        help=('source file or directory. If directory all zip files '
              'will be converted')
    )
    parser.add_argument(
        'destdir', action='store',
        help='output directory'
    )
    parser.add_argument(
        '--workdir', action='store',
        default='/mnt/workdir/nvis_work',
        help=('folder to store working files before moving to final '
              'destination')
    )
    return parser.parse_args()


def main():
    opts = parse_args()
    srcdir = os.path.abspath(opts.srcdir)

    workdir = ensure_directory(opts.workdir)
    dest = ensure_directory(opts.destdir)

    if os.path.isdir(srcdir):
        srcfiles = sorted(glob.glob(os.path.join(srcdir, '*.zip')))
    else:
        srcfiles = [srcfiles]

    # unpack contains one destination datasets
    for srcfile in tqdm.tqdm(srcfiles):
        target_work_dir = create_target_dir(workdir, srcfile)
        try:
            convert(srcfile, target_work_dir)
            target_dir = create_target_dir(dest, srcfile)
            move_files(target_work_dir, target_dir)
        finally:
            shutil.rmtree(target_work_dir)


if __name__ == "__main__":
    main()
