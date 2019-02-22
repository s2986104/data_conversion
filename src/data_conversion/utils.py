import glob
import os
import os.path
import shutil
import gdal
import time
import subprocess


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

