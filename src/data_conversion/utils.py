import glob
import os
import os.path
import re
import shutil
import subprocess
import time

from osgeo import gdal


# convert pixel to projection unit
def transform_pixel(ds, x, y):
    xoff, a, b, yoff, d, e = ds.GetGeoTransform()
    return (
        # round lat/lon to 5 digits which is about 1cm at equator
        round(a * x + b * y + xoff, 5),
        round(d * x + e * y + yoff, 5),
    )


# create directory path if it doesn't exist or check if it is a dir
# returns absolute path to given path
def ensure_directory(path):
    path = os.path.abspath(path)
    if os.path.exists(path) and not os.path.isdir(path):
        raise Exception("Path {} exstis and is not a directory.".format(path))
    # try to create path
    os.makedirs(path, exist_ok=True)
    return path


# move contents of srcdir to destdir
# TODO: some sort of sync would be nice, e.g. remove non existent files in destdir
def move_files(srcdir, destdir):
    srcdir = os.path.abspath(srcdir)
    destdir = os.path.abspath(destdir)
    for fname in glob.glob(os.path.join(srcdir, '*')):
        shutil.move(fname, os.path.join(destdir, os.path.basename(fname)))


def open_gdal_dataset(path):
    """Open a GDAL dataset.

    Try opening the given path, which may be a gdal url (e.g. /vsizip/...)
    and in case of errors back off and retry again for up to 5 minutes?

    return an open gdal.Dataset or None
    """
    attempts = 5
    while attempts:
        # TODO: we could check if the path exists at all in case it is
        #       a normal file path; othewise we'd need to parse the
        #       gdal open string to finde the file name and proto
        ds = gdal.Open(path)
        if ds is None:
            # not open ... sleep for a minute
            # TODO: can we check some sort of error code?
            attempts -= 1
            print("Open {} failed. Try again in a minute. ({} attempts left)".format(path, attempts))
            time.sleep(60)
        else:
            return ds
    return None


def retry_run_cmd(cmd):
    """Run CMD and retry if failed.

    Runs the given command in a subprocess and retries it if it fails.

    raises an exception in case execution still fails.
    """
    attempts = 5
    while attempts:
        try:
            ret = subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            ret.check_returncode()
            return
        except subprocess.CalledProcessError as e:
            attempts -= 1
            print("run cmd failed: {}. Try again in a minute. ({} attempts left)".format(e, attempts))
            time.sleep(60)
    # if we end up here, then the cmd did not succeed
    raise Exception('Subprocess {} failed.'.format(cmd))


def check_file_online(fname):
    """compares allocated block size against actual file size.

    if blk size is 0, then file is offline
    if blk size is lower than file size, then file is being brought online
           and progress can be checked against changes to blk_size over time
    if blk size >= file size then file should be online and ready.
    """
    # 
    st = os.stat(fname)
    # python reports number of blocks in 512 bytes
    blk_size = st.st_blocks * 512
    if blk_size == 0:
        # no blocks allocated, file entirely offline
        return False, blk_size, 'Offline'
    elif blk_size < st.st_size:
        # blocks take less space than file size, file is being brought online
        # or has failed to come online :(
        return False, blk_size, 'Partially Online'
    elif blk_size > st.st_size:
        # more blocks allocated thas file size, file is online
        return True, blk_size, 'Online'


def match_coverage(cov, attrs):
    # used to filter set of coverages
    md = cov['bccvl:metadata']
    # check all filters attrs. if any of the filters does not match return False
    for attr, value in attrs.items():
        if isinstance(value, re.Pattern):
            # if regexp does not match return False
            if not value.match(md[attr]):
                return False
            continue
        if value is None:
            # None means attr should not be there
            if attr in md:
                return False
            continue
        if value == '*':
            # attr should be there with any value, empty or None
            if attr not in md:
                return False
            continue
        if md.get(attr) != attrs[attr]:
            # value must match exactly
            return False
    return True
