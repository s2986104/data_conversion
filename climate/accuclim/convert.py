#!/usr/bin/env python
import os
import os.path
import glob
import json
import tempfile
import shutil
import sys
import re
import zipfile


CURRENT_PREFIX = "accuclim_"
JSON_TEMPLATE = "accu.template.json"


def create_target_dir(basename):
    """create zip folder structure in tmp location.
    return root folder
    """
    tmpdir = tempfile.mkdtemp(prefix=basename)
    os.mkdir(os.path.join(tmpdir, basename))
    os.mkdir(os.path.join(tmpdir, basename, 'data'))
    os.mkdir(os.path.join(tmpdir, basename, 'bccvl'))
    return tmpdir


def gdal_translate(src, dest):
    """Use gdal_translate to copy file from src to dest"""
    cmd = 'gdal_translate -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" {0} {1}'.format(src, dest)
    print cmd
    ret = os.system('gdal_translate -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" {0} {1}'
                    .format(src, dest))
    if ret != 0:
        raise Exception("can't gdal_translate {0} ({1})".format(src,
                                                                ret))


def gdal_calc(src, dest, formula):
    """Use gdal_calc to copy file from src to dest and apply formula"""
    tmpfile = None
    try:
        tmpfile = os.path.join(os.path.dirname(dest), 'calc_{0}'.format(os.path.basename(dest)))
        # rescale values to tempdir
        cmd = ('gdal_calc.py -A {srcfile} --calc="{formula}" --outfile={destfile}'
               .format(srcfile=src,
                       destfile=tmpfile,
                       formula=formula)
        )
        ret = os.system(cmd)
        if ret != 0:
            raise Exception("COMMAND '{}' failed.".format(cmd))
        # copy file to final destination and embed statistics
        ret = os.system('gdal_translate -of GTiff -stats -co "COMPRESS=LZW" -co "TILED=YES" {0} {1}'
                            .format(tmpfile, dest))
        if ret != 0:
            raise Exception("can't gdal_translate {0} ({1})".format(src,
                                                                ret))
    finally:
        # cleanup tempfile
        if tmpfile and os.path.exists(tmpfile):
            os.remove(tmpfile)


def convert(srcdir, ziproot, basename, year):
    """copy all files and convert if necessary to zip preparation dir.
    """
    for i in range(1, 8):
        srcfile = 'accuCLIM_{0:02d}_{1}.tif'.format(i, year)
        vsizip_src_dir = "/vsizip/" + os.path.join(srcdir, year, srcfile)
        # just copy all the others
        destfile = 'accuCLIM_{0:02d}.tif'.format(i)
        gdal_translate(vsizip_src_dir, os.path.join(ziproot, basename, 'data', destfile))


def gen_metadatajson(template, ziproot, basename, year):
    """read metadata template and populate rest of fields
    and write to ziproot + '/bccvl/metadata.json'
    """
    md = json.load(open(template, 'r'))
    
    # Update the title, and year of temporal_coverage
    start_year = int(year) - 14
    md['title'] = md['title'].format(year=year)
    md['temporal_coverage']['start'] = str(start_year)
    md['temporal_coverage']['end'] = str(start_year + 29)

    # update layer info
    md['files'] = {}
    for filename in glob.glob(os.path.join(ziproot, basename, 'data', '*.tif')):
        # get zip root relative path
        zippath = os.path.relpath(filename, ziproot)
        layer_num = re.match(r'.*(\d\d).tif', os.path.basename(filename)).group(1)
	md['files'][zippath] = {
            'layer': 'B{0}'.format(layer_num)
        }
    mdfile = open(os.path.join(ziproot, basename, 'bccvl', 'metadata.json'), 'w')

    json.dump(md, mdfile, indent=4)
    mdfile.close()


def zipbccvldataset(ziproot, destdir, basename):
    zipname = os.path.abspath(os.path.join(destdir, basename + '.zip'))
    ret = os.system('cd {0}; zip -r {1} {2}'.format(ziproot,
                                                    zipname,
                                                    basename))
    if ret != 0:
        raise Exception("can't zip {0} ({1})".format(ziproot, ret))


def main(argv):
    ziproot = None
    srcdir = None
    try:
        if len(argv) != 3:
            print "Usage: {0} <srczip> <destdir>".format(argv[0])
            sys.exit(1)
        srcdir = argv[1]
        destdir = argv[2]
        zf = zipfile.ZipFile(srcdir)
        yearlist = list(set([os.path.dirname(fp) for fp in zf.namelist()]))
        print yearlist
        for year in yearlist: 
            # unpack contains one destination datasets
            base_dir = CURRENT_PREFIX + year
            ziproot = create_target_dir(base_dir)

            convert(srcdir, ziproot, base_dir, year)
            gen_metadatajson(JSON_TEMPLATE, ziproot, base_dir, year)
            zipbccvldataset(ziproot, destdir, base_dir)
            if ziproot:
                shutil.rmtree(ziproot)
    finally:
        # cleanup temp location
        if ziproot and os.path.exists(ziproot):
            shutil.rmtree(ziproot)


if __name__ == '__main__':
    main(sys.argv)
