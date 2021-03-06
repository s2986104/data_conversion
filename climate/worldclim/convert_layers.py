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


# Worldclim current seems to be slightly off, we use this map to adjust it.
GEO_TRANSFORM_PATCH = {
    '10m': (-180.0, 0.16666666666666666, 0.0, 90.0, 0.0, -0.16666666666666666),
    '2-5m': (-180.0, 0.041666666666667, 0.0, 90.0, 0.0, -0.041666666666667),
    '5m': (-180.0, 0.083333333333333, 0.0, 90.0, 0.0, -0.083333333333333),
    '30s': (-180.0, 0.008333333333333, 0.0, 90.0, 0.0, -0.008333333333333),
}


def parse_zip_filename(srcfile):
    """
    srcfile should be an absolute path name to a zip file in the source folder
    """
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
        res = basedir.replace(".", '-')
        time_ = 'future'
        type_ = 'tif'
        year = 2000 + int(year)
        emsc = EMSC_MAP[emsc]
        gcm = GCM_MAP[gcm]
    return time_, gcm, emsc, year, var, res, type_


def gdal_options(srcfile):
    """
    options to add metadata for the tiff file
    """
    time_, gcm, emsc, year, _, _, _ = parse_zip_filename(srcfile)

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
    options += ['-mo', 'version=1.4']
    return options


def get_layer_id(lzid, filename):
    """
    lzid     ... overall layerid from zip file
    filename ... inside zip
    """
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
        raise Exception('Unknown lzid {}'.format(lzid))
    return layerid, month


def run_gdal(cmd, infile, outfile, layerid, res):
    _, tfname = tempfile.mkstemp(suffix='.tif')
    try:
        retry_run_cmd(cmd + [infile, tfname])
        # add band metadata
        ds = gdal.Open(tfname, gdal.GA_Update)
        # Patch GeoTransform ... at least worldclim current data is
        #                        slightly off
        ds.SetGeoTransform(GEO_TRANSFORM_PATCH[res])
        # For some reason we have to flust the changes to geo transform
        # immediately otherwise gdal forgets about it?
        # TODO: check if setting ds = None fixes this as well?
        ds.FlushCache()
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
    _, _, _, _, var, res, type_ = parse_zip_filename(srcfile)

    pool = futures.ProcessPoolExecutor(2)
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
            layerid, month = get_layer_id(var, os.path.basename(zipinfo.filename.rstrip('/')))
            # replace '_' in layerid to '-' for filename generation
            file_part = layerid.replace('_', '-')
            if month is not None:
                file_part = '{}-{:02d}'.format(file_part, month)
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
            results.append(pool.submit(run_gdal, cmd, srcurl, destpath, var, res))
            # run_gdal(cmd, srcurl, destpath, var, res)

    for result in tqdm.tqdm(futures.as_completed(results), desc=os.path.basename(srcfile), total=len(results)):
        if result.exception():
            print("Job failed")
            raise result.exception()


def create_target_dir(destdir, srcfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    time_, gcm, emsc, year, var, res, _ = parse_zip_filename(srcfile)
    if time_ == 'current':
        dirname = '_'.join(('worldclim', res, var))
    else:
        dirname = '_'.join((emsc, gcm, str(year), res, var))
    root = os.path.join(destdir, time_, dirname)
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
        default='/mnt/workdir/worldclim_work',
        help=('folder to store working files before moving to final '
              'destination')
    )
    parser.add_argument(
        '--resolution', action='append',
        choices=['10m', '5m', '2.5m', '30s'],
        help='only convert files at specified resolution'
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
        if opts.resolution:
            # Note: this regexp works only for the current naming scheme of
            #       worldclim 1.4 files
            fmatch = re.compile(r'|'.join(opts.resolution))
            srcfiles = sorted(
                name for name in glob.glob(os.path.join(src, '**/*.zip'), recursive=True)
                if fmatch.search(name)
            )
        else:
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
