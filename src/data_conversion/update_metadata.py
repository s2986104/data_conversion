import argparse
from concurrent.futures import ThreadPoolExecutor
import glob
import logging
import os
import os.path
import shutil
import subprocess
import tempfile

from osgeo import gdal

from data_conversion.vocabs import VAR_DEFS, PREDICTORS

POOL = ThreadPoolExecutor(max_workers=4)


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('srcdir', help='Source directory')
    parser.add_argument('--dry-run', action='store_true', help='Dry Run')
    parser.add_argument('--force', action='store_true', help='Re-encode anyway')
    parser.add_argument('--overviews', action='store_true', default=False, help='Generate internal overviews')
    parser.add_argument('--blocksize', nargs=1, default=256, type=int, choices=[256, 512], help='internal tile Blocksize')
    parser.add_argument('--nopredictors', action='store_false', help='Use no predictors', dest='predictors')
    return parser.parse_args()

def update_metadata(fname, opts):
    log = logging.getLogger(__name__)
    # prefix for logs
    prefix = os.path.basename(fname)

    # check if update is needed
    ds = gdal.Open(fname)
    bd = ds.GetRasterBand(1)
    md = bd.GetMetadata_Dict()
    # remove units
    changed = md.pop('units', None) is not None

    var_id = md.get('standard_name')
    if not var_id:
        log.warn('{}: Unknown variable {}'.format(prefix, var_id))
        return
    var_def = VAR_DEFS[var_id]
    changed = changed or (bd.GetUnitType() != var_def['units'])
    # check overviews as well
    changed = changed or (opts.overviews and (bd.GetOverviewCount() == 0))
    # check blk size
    changed = changed or ([opts.blocksize, opts.blocksize] != bd.GetBlockSize())

    if not (opts.force or changed):
        log.info("{}: Unchanged".format(prefix))
        return
    # free resources
    bd = None
    ds = None

    # update metadata
    with tempfile.NamedTemporaryFile() as tmpf:
        # 1. make a copy
        subprocess.check_call(
            ['cp', fname, tmpf.name],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        old_size = os.path.getsize(tmpf.name)
        # 2. open and modify tif
        ds = gdal.Open(tmpf.name, gdal.GA_Update)
        bd = ds.GetRasterBand(1)
        dataType = bd.DataType
        bd.SetMetadata(md)
        bd.SetUnitType(var_def['units'])
        # TODO: resampling nearest for INT types, otherwise CUBIC
        if opts.overviews:
            overviews = []
            size = max(ds.RasterXSize, ds.RasterYSize)
            n = 2
            while (size > opts.blocksize):
                overviews.append(n)
                size = size // 2
                n = n * 2
            ds.BuildOverviews(overviewlist=overviews)
        else:
            ds.BuildOverviews()
        # ensure ds is closed and flushed
        ds.FlushCache()
        bd = None
        ds = None
        del ds
        # 4. gdal_translate and compress
        cmd = [
            'gdal_translate',
            '-of', 'GTiff',
            # 512 blocksize slightly less efficient in compressing?
            '-co', 'BLOCKXSIZE=256', '-co', 'BLOCKYSIZE=256',
            '-co', 'TILED=yes', '-co', 'COPY_SRC_OVERVIEWS=YES',
            '-co', 'COMPRESS=DEFLATE',
        ]
        out_files = [
            tempfile.NamedTemporaryFile()
        ]

        cmds = [
            cmd + [tmpf.name, out_files[0].name],
        ]
        if opts.predictors:
            out_files.append(tempfile.NamedTemporaryFile())
            cmds.append(
                cmd + [
                    '-co', 'PREDICTOR={}'.format(PREDICTORS[dataType]),
                    tmpf.name, out_files[1].name
                ] 
            )
        for cmd in cmds:
            log.debug('{}: run {}'.format(prefix, cmd))
            subprocess.check_call(
                cmd,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        # check out put file sizes
        out_sizes = [os.path.getsize(x.name) for x in out_files]
        out_idx = out_sizes.index(min(out_sizes))
        if out_idx == 0:
            log.info("{}: No Predictor: {} -> {}".format(prefix, old_size, out_sizes[out_idx]))
        else:
            log.info("{}: Predictor: {} -> {}".format(prefix, old_size, out_sizes[out_idx]))
        # copy back
        if not opts.dry_run:
            subprocess.check_call(
                ['cp', out_files[out_idx].name, fname],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        log.debug("{}: done".format(prefix))


from itertools import zip_longest, cycle

def main():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    opts = parse_args()
    opts.srcdir = os.path.abspath(opts.srcdir)

    globs = glob.iglob(os.path.join(opts.srcdir, '**', '*.tif'), recursive=True)
    slices = [globs] * 8  # chunk size
    for fnames in zip_longest(*slices):
        # remove all None entries from zip_longest
        for res in POOL.map(update_metadata, filter(None, fnames), cycle([opts])):
            # test if ok
            pass




# starting from 3.1:
# gdal_translate in.tif out.tif -of COG -co
# -co OVERVIEWS=AUTO
# -co RESAMPLING=NEAREST ... might be necessary to force this on categorical for overviews
