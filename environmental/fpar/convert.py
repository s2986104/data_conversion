#!/usr/bin/env python

# This script may use a lot of temp space.
# Make sure to set TMPDIR / TEMP / TMP envvar to some location with enough free space

import os
import os.path
import zipfile
import glob
import json
import tempfile
import shutil
import sys
import re
from collections import namedtuple
import calendar
import fpar_stats


JSON_TEMPLATE = 'fpar.template.json'

def ungz(filename, destfile):
    """gunzip given filename.
    """
    _gunzip = 'gunzip -c {0} > {1}'.format(filename, destfile)
    ret = os.system(_gunzip)
    if ret != 0:
        raise Exception("can't gunzip {0} ({1})".format(filename, ret))
    return destfile


def gen_metadatajson(src, dest):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """
    md = json.load(open(JSON_TEMPLATE, 'r'))
    md[u'files'] = {}
    for filename in glob.glob(os.path.join(dest, '*', '*.tif')):
        base = os.path.basename(filename)
        m = re.match(r'fpar\.(....)\.(..)\.*', base)
        year = m.group(1)
        month = int(m.group(2))
        layer_id = 'FPAR{:02d}'.format(month)
        md[u'title'] = md[u'title'].format(month=calendar.month_name[month], year=year)
        filename = filename[len(os.path.dirname(dest)):].lstrip('/')
        md[u'files'][filename] = {
            u'layer': layer_id,
        }
    mdfile = open(os.path.join(dest, 'bccvl', 'metadata.json'), 'w')
    json.dump(md, mdfile, indent=4)
    mdfile.close()


def create_target_dir(destdir, destfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    root = os.path.join(destdir, destfile)
    os.makedirs(root)
    os.mkdir(os.path.join(root, 'data'))
    os.mkdir(os.path.join(root, 'bccvl'))
    return root

def zip_dataset(ziproot, dest):
    workdir = os.path.dirname(ziproot)
    zipdir = os.path.basename(ziproot)
    zipname = os.path.abspath(os.path.join(dest, zipdir + '.zip'))
    ret = os.system(
        'cd {0}; zip -r {1} {2} -x *.aux.xml*'.format(workdir, zipname, zipdir)
    )
    if ret != 0:
        raise Exception("can't zip {0} ({1})".format(ziproot, ret))
    return zipname


def scale_down(tiffile):
    # scale down the raster data by 10000, and save as float.
    tmpfile = os.path.join(os.path.dirname(tiffile), 'result.tif')
    calc = 'gdal_calc.py -A {0} --outfile={1} --calc="A/10000.0" --type="Float32" --overwrite --co "COMPRESS=LZW" --co "TILED=YES"'.format(tiffile, tmpfile)
    ret = os.system(calc)
    if ret != 0:
        raise Exception("fail to scale down {0} ({1})".format(tiffile, ret))
    # replace source file
    shutil.move(tmpfile, tiffile)


def main(argv):
    year_range = map(str, xrange(2000,2015))
    if len(argv) > 1:
        if argv[1] not in year_range:
            print "Usage: {0} [year]".format(argv(0))
            print "Valid years: {}".format(','.join(year_range))
            sys.exit(1)
        year_range = [ argv[1] ]

    srcfolder = 'source/fpar'
    destfolder = 'bccvl'
    ziproot = None
    #tmpdir = tempfile.mkdtemp(prefix="fpar_")
    tmpdir = os.path.join(tempfile.gettempdir(), 'fparhgay_7')

    try:
        for year in (): # year_range:
            for monthfile in glob.glob('{}/fpar.{}.*.gz'.format(srcfolder, year)):
                try:
                    tifname, _ = os.path.splitext(os.path.basename(monthfile))
                    destbase, _ = os.path.splitext(tifname)
                    # create bccvl target dir
                    ziproot = create_target_dir(tmpdir, destbase)
                    # unzip source to temp location
                    tmptiff = os.path.join(tmpdir, tifname)
                    ungz(monthfile, tmptiff)
                    # re-scale values
                    scale_down(tmptiff)
                    # copy result to destination
                    shutil.copyfile(tmptiff, os.path.join(ziproot, 'data', tifname))
                    # delete tmptiff
                    # os.remove(tmptiff)
                    # generate metadata.json
                    gen_metadatajson(srcfolder, ziproot)
                    # package up zip
                    zipdataset = zip_dataset(ziproot, destfolder)
                    # clean up work dir
                    shutil.rmtree(ziproot)
                    # move zip file to destination
                    shutil.move(zipdataset, destfolder)
                except Exception as e:
                    print "Error: ", e
                    raise e

        # Calculate the fpar statistics for the tiff files
        fpar_stats.fpar_stats(destfolder ,tmpdir, tmpdir)

    finally:
        if tmpdir and os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)


if __name__ == "__main__":
    main(sys.argv)
