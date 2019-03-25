#!/usr/bin/env python
import os
import os.path
import zipfile
import glob
import shutil
import re
import argparse
from concurrent import futures
import tempfile

from osgeo import gdal
import tqdm


from data_conversion.vocabs import VAR_DEFS, PREDICTORS
from data_conversion.utils import ensure_directory, move_files, retry_run_cmd


def parse_zip_filename(srcfile):
    """
    srcfile should be an absolute path name to a zip file in the source folder
    """
    basename, _ = os.path.splitext(os.path.basename(srcfile))
    parts = basename.split('_')
    emsc = 'SRES-' + parts[1]
    # To remove decimal point in CM2.0
    if len(parts) > 3:
        parts[3] = parts[3].replace('.', '')
    gcm = '-'.join(parts[2:]) 
    return gcm, emsc


def gdal_options(srcfile, year):
    """
    options to add metadata for the tiff file
    """
    gcm, emsc = parse_zip_filename(srcfile)

    options = ['-of', 'GTiff', '-co', 'TILED=YES']
    options += ['-co', 'COMPRESS=DEFLATE', '-norat']
    year = int(year)
    # worldclim future spans 30 years
    years = [year - 14, year + 15]
    options += ['-mo', 'emission_scenario={}'.format(emsc)]
    options += ['-mo', 'general_circulation_models={}'.format(gcm.upper())]
    options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
    options += ['-mo', 'year={}'.format(year)]
    return options


def get_layer_id(filename):
    """
    lzid     ... overall layerid from zip file
    filename ... inside zip i.e. TASCLIM_ECHAM5_10_2080.tif
    """
    parts = os.path.splitext(os.path.basename(filename))[0].split('_')
    _, _, layerid, year = parts
    return 'bioclim_{:02d}'.format(layerid), int(year)


def run_gdal(cmd, infile, outfile, layerid):
    _, tfname = tempfile.mkstemp(suffix='.tif')
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
        #band.SetScale(SCALES.get(layerid, 1))
        # band.SetOffset(0.0)
        ds.FlushCache()
        # build cmd
        cmd = [
            'gdal_translate',
            '-of', 'GTiff',
            '-co', 'TILED=yes',
            '-co', 'COPY_SRC_OVERVIEWS=YES',
            '-co', 'COMPRESS=DEFLATE',
            '-co', 'PREDICTOR={}'.format(PREDICTORS[band.DataType]),
        ]
        # check crs
        # Worldclim future datasets have incomplete projection information
        # let's force it to a known proj info anyway
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
    """
    convert all files within srcfile (it's a zip) into destdir
    """
    # parse info from filename
    gcm, emsc = parse_zip_filename(srcfile)

    pool = futures.ProcessPoolExecutor(2)
    results = []

    with zipfile.ZipFile(srcfile) as srczip:
        for zipinfo in tqdm.tqdm(srczip.filelist, desc="build jobs"):
            # there should be tiffs inside
            if zipinfo.is_dir():
                # ignore folders
                continue
            if not zipinfo.filename.endswith('.tif'):
                # ignore non tiff files
                continue
            srcurl = '/vsizip/' + srcfile + '/' + zipinfo.filename
            # format month if needed i.e. CLIMOND_A1B_CSIRO-Mk3.0_25_2100.tif
            layerid, year = get_layer_id(os.path.basename(zipinfo.filename.rstrip('/')))
            # replace '_' in layerid to '-' for filename generation
            file_part = layerid.replace('_', '-')
            destfilename = '_'.join((
                os.path.basename(destdir),
                str(year)   , 
                file_part,
                '.tif'
            ))
            gdaloptions = gdal_options(srcfile, year)
            # output file name
            destpath = os.path.join(destdir, destfilename)
            # run gdal translate
            cmd = ['gdal_translate']
            cmd.extend(gdaloptions)
            results.append(pool.submit(run_gdal, cmd, srcurl, destpath, layerid))
            # run_gdal(cmd, srcurl, destpath, var, res)

    for result in tqdm.tqdm(futures.as_completed(results), desc=os.path.basename(srcfile), total=len(results)):
        if result.exception():
            print("Job failed")
            raise result.exception()


def create_target_dir(destdir, srcfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    gcm, emsc = parse_zip_filename(srcfile)
    dirname = '_'.join(('tasclim', emsc, gcm))
    root = os.path.join(destdir, dirname)
    return ensure_directory(root)


def parse_args():
    """
    parse cli
    """
    parser = argparse.ArgumentParser(
        description='Convert WorldClim current datasets'
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
        default='/mnt/workdir/tasclim_work',
        help=('folder to store working files before moving to final '
              'destination')
    )
    return parser.parse_args()


def main():
    """
    main method
    """
    opts = parse_args()
    src = os.path.abspath(opts.srcdir)

    workdir = ensure_directory(opts.workdir)
    dest = ensure_directory(opts.destdir)

    if os.path.isdir(src):
        srcfiles = sorted(glob.glob(os.path.join(src, '**/*.zip'), recursive=True))
    else:
        srcfiles = [src]

    for srcfile in tqdm.tqdm(srcfiles):
        target_work_dir = create_target_dir(workdir, srcfile)
        try:
            # convert files into workdir
            convert(srcfile, target_work_dir)
            # move results into destination
            target_dir = create_target_dir(dest, srcfile)
            move_files(target_work_dir, target_dir)
        finally:
            # cleanup target_work_dir
            # TODO: this cleans only lowest level subdir, and leaves
            #       intermediary dirs
            shutil.rmtree(target_work_dir)


if __name__ == "__main__":
    main()
