#!/usr/bin/env python
from concurrent import futures
import glob
import os
import os.path
import sys
import tempfile
import zipfile

from osgeo import gdal
import tqdm

from data_conversion.vocabs import VAR_DEFS, PREDICTORS
from data_conversion.utils import ensure_directory, move_files, retry_run_cmd

LAYERINFO = {
    'clay30': ('clay30',2011),
    'asc': ('asc', 2012),
    'pawc_1m': ('pawc_1m', 2014),
    'ph': ('ph_0_30', 2014),
    'bd30': ('bd30', 2011)
}

def gdal_options(srcfile, year):
    # options to add metadata for the tiff file 

    options = ['-of', 'GTiff', '-co', 'TILED=YES']
    options += ['-co', 'COMPRESS=DEFLATE', '--config', 'GDAL_PAM_MODE', 'PAM']
    options += ['-mo', 'year_range={}-{}'.format(year, year)]
    options += ['-mo', 'year={}'.format(year)]
    return options


def get_layer_id(filename):
    layerid = os.path.splitext(os.path.basename(filename))[0]
    return layerid


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
        fname = get_layer_id(os.path.basename(srcfile))
        layerid, year = LAYERINFO[fname.lower()]
        if layerid == 'asc':
            esrifname = '/'.join([fname, 'hdr.adf'])
        else:
            esrifname = '/'.join([fname, layerid, 'hdr.adf'])
        for zipinfo in tqdm.tqdm(srczip.filelist, desc="build jobs"):   
            if zipinfo.is_dir():
                # skip dir entries
                continue
            if zipinfo.filename != esrifname:
                # skip non .asc files
                continue

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
            results.append(pool.submit(run_gdal, cmd, srcurl, destpath, layerid))

    for result in tqdm.tqdm(futures.as_completed(results), desc=os.path.basename(srcfile), total=len(results)):
        if result.exception():
            print("Job failed")
            raise result.excption()


def create_target_dir(destdir, srcfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    root = os.path.join(destdir, 'nsg-2011-250m')
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
