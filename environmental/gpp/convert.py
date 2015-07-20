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

JSON_TEMPLATE = 'gpp.template.json'
TITLE_TEMPLATE = u'Gross Primary Productivity for {} ({})'

FOLDERS = {
    'gpp_maxmin_2000_2007': 'min, max & mean',
    'GPP_year_means_00_07': 'annual mean',
}

LAYER_MAP = {
    # gpp_maxmin_2000_2007
    'gppyrmax_2000_07_molco2yr.tif':    ('GPP1', '2000-2007'),
    'gppyrmean_2000_07_molco2yr.tif':   ('GPP2', '2000-2007'),
    'gppyrmin_2000_07_molco2yr.tif':    ('GPP3', '2000-2007'),
    
    # gpp_year_means2000_2007
    'gppyr_2000_01_molco2m2yr_m.tif':   ('GPP4', '2000'),
    'gppyr_2001_02_molco2m2yr_m.tif':   ('GPP4', '2001'),
    'gppyr_2002_03_molco2m2yr_m.tif':   ('GPP4', '2002'),
    'gppyr_2003_04_molco2m2yr_m.tif':   ('GPP4', '2003'),
    'gppyr_2004_05_molco2m2yr_m.tif':   ('GPP4', '2004'),
    'gppyr_2005_06_molco2m2yr_m.tif':   ('GPP4', '2005'),
    'gppyr_2006_07_molco2m2yr_m.tif':   ('GPP4', '2006'),
}


def convert(filename, dest):
    """convert .rst files to .tif in dest
    """
    srcfile = os.path.splitext(os.path.basename(filename))[0].lower()
    destfile = '{}.tif'.format(srcfile)
    if destfile in LAYER_MAP:
        print "Converting {}".format(filename)
        destpath = os.path.join(dest, 'data', destfile)
        ret = os.system(
            'gdal_translate -of GTiff {0} {1}'.format(filename, destpath)
        )
        if ret != 0:
            raise Exception(
                "can't gdal_translate {0} ({1})".format(filename, ret)
            )
    else:
        print "Skipping {}".format(filename)

def gen_metadatajson(src, dest):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """
    md = json.load(open(JSON_TEMPLATE, 'r'))
    md[u'files'] = {}
    for filename in glob.glob(os.path.join(dest, '*', '*.tif')):
        layer_id, time_range = LAYER_MAP[os.path.basename(filename)]
        md[u'title'] = TITLE_TEMPLATE.format(time_range, FOLDERS[src])   
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
    ziproot = None
    if len(argv) != 3:
        print "Usage: {0} <srcdir> <destdir>".format(argv[0])
        sys.exit(1)
    src  = argv[1]
    srcfolder = os.path.basename(src)
    if srcfolder not in FOLDERS:
        print "Folder unknown, valid options are {}".format(', '.join(FOLDERS))
        return
    dest = argv[2]
    if srcfolder == 'gpp_maxmin_2000_2007':
        destfile = srcfolder.lower()
        try:
            ziproot = create_target_dir(dest, destfile)
            for srcfile in glob.glob(os.path.join(src, '*.[Rr][Ss][Tt]')):
                convert(srcfile, ziproot)
            gen_metadatajson(srcfolder, ziproot)
            zip_dataset(ziproot, dest)
        finally:
            if ziproot:
                shutil.rmtree(ziproot)
    elif srcfolder == 'GPP_year_means_00_07':
        for srcfile in glob.glob(os.path.join(src, '*.[Rr][Ss][Tt]')):
            destfile = os.path.splitext(os.path.basename(srcfile))[0].lower()
            try:
                ziproot = create_target_dir(dest, destfile)
                convert(srcfile, ziproot)
                gen_metadatajson(srcfolder, ziproot)
                zip_dataset(ziproot, dest)
            finally:
                if ziproot:
                    shutil.rmtree(ziproot)

if __name__ == "__main__":
    main(sys.argv)

