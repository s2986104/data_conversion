#!/usr/bin/env python
import os
import os.path
import zipfile
import glob
import sys
import re
import argparse
import tqdm
from concurrent import futures
import tempfile
import subprocess
from osgeo import gdal


from data_conversion import VAR_DEFS, PREDICTORS


EMSC_MAP = {
    '26': 'RCP2.6',
    '45': 'RCP4.5',
    '60': 'RCP6.0',
    '85': 'RCP8.5'
}

GCM_MAP = {
    'ac': 'ACCESS1-0',  # non-commercial
    'bc': 'BCC-CSM-1',
    'cc': 'CCSM4',
    'ce': 'CESM1-CAM5-1-FV2',
    'cn': 'CNRM-CM5',
    'gf': 'GFDL-CM3',
    'gd': 'GFDL-ESM2G',
    'gs': 'GISS-E2-R',
    'hd': 'HadGEM2-AO',
    'hg': 'HadGEM2-CC',
    'he': 'HadGEM2-ES',
    'in': 'INMCM4',
    'ip': 'IPSL-CM5A-LR',
    'mi': 'MIROC-ESM-CHEM',  # non-commercial
    'mr': 'MIROC-ESM',  # non-commercial
    'mc': 'MIROC5',  # non-commercial
    'mp': 'MPI-ESM-LR',
    'mg': 'MRI-CGCM3',
    'no': 'NorESM1-M',
}

SCALES = {
    'tmean': 0.1,
    'tmin': 0.1,
    'tmax': 0.1,
    'bioclim_01': 0.1,
    'bioclim_02': 0.1,
    # 'bioclim_03': 0.1,
    'bioclim_04': 0.1,
    'bioclim_05': 0.1,
    'bioclim_06': 0.1,
    'bioclim_07': 0.1,
    'bioclim_08': 0.1,
    'bioclim_09': 0.1,
    'bioclim_10': 0.1,
    'bioclim_11': 0.1,
}


def parse_zip_filename(srcfile):
    # srcfile should be an absolute path name to a zip file in the source folder
    basename, _ = os.path.splitext(os.path.basename(srcfile))
    basedir = os.path.basename(os.path.dirname(srcfile))
    if basedir == 'current':
        # it's a current file ... type_ = 'esri'
        var, res, type_ = basename.split('_')
        time_ = 'current'
        gcm = emsc = None
        year = int('1975')
    else:
        # it's future .. basedir is resolution
        gcm, emsc, var, year = re.findall(r'\w{2}', basename)
        res = basedir
        time_ = 'future'
        type_ = 'tif'
        year = 2000 + int(year)
        emsc = EMSC_MAP[emsc]
        gcm = GCM_MAP[gcm]
    return time_, gcm, emsc, year, var, res, type_


def gdal_options(srcfile):
    # options to add metadata for the tiff file
    time_, gcm, emsc, year, var, res, type_ = parse_zip_filename(srcfile)

    options = ['-of', 'GTiff', '-co', 'TILED=YES']
    options += ['-co', 'COMPRESS=DEFLATE', '-norat']
    if time_ == 'current':
        # worldclim current is over 30 year time span
        years = [year - 14, year + 15]
        options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
        options += ['-mo', 'year={}'.format(year)]
    else:
        year = int(year)
        # worldclim future spans 10 years
        years = [year - 9, year + 10]
        options += ['-mo', 'emission_scenario={}'.format(emsc)]
        options += ['-mo', 'general_circulation_models={}'.format(gcm.upper())]
        options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
        options += ['-mo', 'year={}'.format(year)]
    return options


def get_layer_id(lzid, filename):
    # lzid ... overall layerid from zip file
    # fielname inside zip
    # check month
    month = None
    if lzid in ('prec', 'tmin', 'tmax', 'tmean'):
        # current other
        layerid = lzid
        month = int(filename.split('_')[1])
    elif lzid == 'alt':
        # current alt
        layerid = lzid
    elif lzid == 'bio':
        # current dataset
        layerid = 'bioclim_{:02d}'.format(int(filename.split('_')[1]))
    elif lzid == 'bi':
        # future
        # last one or two digits befire '.tif' are bioclim number
        layerid = 'bioclim_{:02d}'.format(int(filename[8:-4]))
    elif lzid == 'pr':
        # future
        layerid = 'prec'
        month = int(filename[8:-4])
    elif lzid == 'tn':
        # future
        layerid = 'tmin'
        month = int(filename[8:-4])
    elif lzid == 'tx':
        # future
        layerid = 'tmax'
        month = int(filename[8:-4])
    else:
        raise Exception('Unknown lzid {}'.fromat(lzid))
    return layerid, month


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
        # adapt layerid from zip file to specific layer inside zip
        layerid, month = get_layer_id(layerid, os.path.basename(infile))
        if month:
            ds.SetMetadataItem('month', str(month))
        band = ds.GetRasterBand(1)
        # ensure band stats
        band.ComputeStatistics(False)
        for key, value in VAR_DEFS[layerid].items():
            band.SetMetadataItem(key, value)
        # just for completeness
        band.SetUnitType(VAR_DEFS[layerid]['units'])
        band.SetScale(SCALES.get(layerid, 1))
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
    """
    """
    # parse info from filename
    time_, gcm, emsc, year, var, res, type_ = parse_zip_filename(srcfile)

    pool = futures.ProcessPoolExecutor()
    results = []

    with zipfile.ZipFile(srcfile) as srczip:
        for zipinfo in tqdm.tqdm(srczip.filelist, desc="build jobs"):
            if type_ == 'esri':
                # we look for folders with a 'hdr.adf' file inside
                if not zipinfo.is_dir():
                    # ingore files
                    continue
                if zipinfo.filename + 'hdr.adf' not in srczip.namelist():
                    # ignore this folder no data inside
                    continue
                # gdal doesn't like trailing slashes
                srcurl = '/vsizip/' + srcfile + '/' + zipinfo.filename.rstrip('/')
            else:
                # there should be tiffs inside
                if zipinfo.is_dir():
                    # ignore folders
                    continue
                if not zipinfo.filename.endswith('.tif'):
                    # ignore non tiff files
                    continue
                srcurl = '/vsizip/' + srcfile + '/' + zipinfo.filename
            # format month if needed
            file_part, month = get_layer_id(var, os.path.basename(zipinfo.filename.rstrip('/')))
            if month is not None:
                file_part = '{}_{:02d}'.format(file_part, month)
            destfilename = (
                os.path.basename(destdir) +
                '_' +
                file_part +
                '.tif'
            )
            gdaloptions = gdal_options(srcfile)
            # output file name
            destpath = os.path.join(destdir, destfilename)
            # run gdal translate
            cmd = ['gdal_translate']
            cmd.extend(gdaloptions)
            results.append(pool.submit(run_gdal, cmd, srcurl, destpath, var))

    for result in tqdm.tqdm(futures.as_completed(results), desc=os.path.basename(srcfile), total=len(results)):
        if result.exception():
            print("Job failed")
            raise result.exception()

    return


def create_target_dir(destdir, srcfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    time_, gcm, emsc, year, var, res, type_ = parse_zip_filename(srcfile)
    if time_ == 'current':
        dirname = '_'.join(('worldclim', res, var))
    else:
        dirname = '_'.join((emsc, gcm, str(year), res, var))
    root = os.path.join(destdir, time_, dirname)
    os.makedirs(root, exist_ok=True)
    return root


def main():
    parser = argparse.ArgumentParser(description='Convert WorldClim current datasets')
    parser.add_argument('srcdir', type=str, help='source file or directory. If directory all zip files will be converted')
    parser.add_argument('destdir', type=str, help='output directory')
    opts = parser.parse_args()

    src = os.path.abspath(opts.srcdir)
    dest = os.path.abspath(opts.destdir)

    # fail if destination exists but is not a directory
    if os.path.exists(dest) and not os.path.isdir(dest):
        print("Path {} exists and is not a directory.".format(dest))
        sys.exit(os.EX_IOERR)

    # try to create estination if it doesn't exist
    try:
        os.makedirs(os.path.abspath(dest), exist_ok=True)
    except Exception as e:
        print("Failed to create directory at {}.".format(dest))
        sys.exit(os.EX_IOERR)

    if os.path.isdir(src):
        srcfiles = sorted(glob.glob(os.path.join(src, '**/*.zip'), recursive=True))
    else:
        srcfiles = [src]

    for srcfile in tqdm.tqdm(srcfiles):
        targetdir = create_target_dir(dest, srcfile)
        convert(srcfile, targetdir)


if __name__ == "__main__":
    main()
