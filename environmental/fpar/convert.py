#!/usr/bin/env python
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

JSON_TEMPLATE = 'fpar.template.json'

def ungz(filename, destfile):
    """gunzip given filename.
    """
    _gunzip = 'gunzip -c {0} > {1}'.format(filename, destfile)
    ret = os.system(_gunzip)
    if ret != 0:
        raise Exception("can't gunzip {0} ({1})".format(filename, ret))


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
        layer_id = 'FPAR{}'.format(month)
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
    os.mkdir(root)
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

    for year in year_range:
        for monthfile in glob.glob('{}/fpar.{}.*.gz'.format(srcfolder, year)):
            try:
                destfile = os.path.basename(monthfile).replace('.tif.gz','')
                ziproot = create_target_dir(destfolder, destfile)
                desttif = os.path.splitext(os.path.split(monthfile)[1])[0]
                ungz(monthfile, os.path.join(ziproot, 'data', desttif))
                gen_metadatajson(srcfolder, ziproot)
                zip_dataset(ziproot, destfolder)
            finally:
                if ziproot:
                    shutil.rmtree(ziproot)

if __name__ == "__main__":
    main(sys.argv)

