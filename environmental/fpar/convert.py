#!/usr/bin/env python

# This script may use a lot of temp space.
# Make sure to set TMPDIR / TEMP / TMP envvar to some location with enough free space

import os
import os.path
import glob
import json
import tempfile
import shutil
import sys
import re
import calendar
import fpar_stats


JSON_TEMPLATE = 'fpar.template.json'

def ungz(filename):
    """gunzip given filename.
    """
    # unzip file
    _gunzip = 'gunzip {0}'.format(filename)
    ret = os.system(_gunzip)
    if ret != 0:
        raise Exception("can't gunzip {0} ({1})".format(filename, ret))
    # get rid of .gz extension
    filename, _ = os.path.splitext(filename)
    return filename


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
    # remove orig file
    os.remove(tiffile)
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
    tmpdir = tempfile.mkdtemp(prefix="fpar_")

    try:
        # prepare fpar files if necessary
        for gzfile in glob.glob('{}/*.gz'.format(srcfolder)):
            tiffile = ungz(gzfile)
            # re-scale source files
            scale_down(tiffile)
        for year in year_range:
            for monthfile in glob.glob('{}/fpar.{}.*.tif'.format(srcfolder, year)):
                try:
                    tifname = os.path.basename(monthfile)
                    destbase, _ = os.path.splitext(tifname)
                    # create bccvl target dir
                    ziproot = create_target_dir(tmpdir, destbase)
                    # copy result to destination
                    shutil.copyfile(monthfile, os.path.join(ziproot, 'data', tifname))
                    # generate metadata.json
                    gen_metadatajson(srcfolder, ziproot)
                    # package up zip in destination
                    zip_dataset(ziproot, destfolder)
                    # clean up work dir
                    shutil.rmtree(ziproot)
                except Exception as e:
                    print "Error: ", e
                    raise

        # Calculate the fpar statistics for the tiff files
        fpar_stats.fpar_stats(destfolder ,tmpdir, srcfolder)

    finally:
        # clean up all tempspace
        if tmpdir and os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)


if __name__ == "__main__":
    main(sys.argv)
