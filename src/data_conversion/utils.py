import glob
import os
import os.path
import shutil


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
