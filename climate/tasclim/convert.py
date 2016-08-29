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


JSON_TEMPLATE = "tas.template.json"

SRC_FILE_MAP = {
    'A2_ECHAM5':    'TASCLIM_ECHAM5',
    'A2_GCM_MEAN': 'TASCLIM',
    'A2_GFDL-CM2.0': 'TASCLIM_GFDL-CM2.0',
    'A2_GFDL-CM2.1': 'TASCLIM_GFDL-CM2.1',
    'A2_MIROC3.2_MEDRES': 'TASCLIM_MIROC3.2_MEDRES',
    'A2_UKMO-HadCM3': 'TASCLIM_UKMO-HadCM3',
    'B1_ECHAM5':    'TASCLIM_ECHAM5',
    'B1_GCM_MEAN': 'TASCLIM',
    'B1_GFDL-CM2.0': 'TASCLIM_GFDL-CM2.0',
    'B1_GFDL-CM2.1': 'TASCLIM_GFDL-CM2.1',
    'B1_MIROC3.2_MEDRES': 'TASCLIM_MIROC3.2_MEDRES',
    'B1_UKMO-HadCM3': 'TASCLIM_UKMO-HadCM3'
}


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
    ret = os.system('gdal_translate -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" {0} {1}'
                    .format(src, dest))
    if ret != 0:
        raise Exception("can't gdal_translate {0} ({1})".format(src, ret))

def convert(srcdir, ziproot, basename, year, filename):
    """copy all files and convert if necessary to zip preparation dir.
    """
    for i in range(1, 20):
        srcfile = '{0}_{1:02d}_{2}.tif'.format(SRC_FILE_MAP[filename], i, year)
        vsizip_src_dir = "/vsizip/" + os.path.join(srcdir, year, srcfile)
        # just copy all the files
        destfile = 'TASCLIM_{0:02d}.tif'.format(i)
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
        fname, ext = os.path.splitext(os.path.basename(srcdir))

        # Replace the short emsc with full emsc name
        if fname.startswith('A2_'):
            dest_filename = fname.replace('A2', 'TASCLIM_SRES-A2', 1)
        elif fname.startswith('B1_'):
            dest_filename = fname.replace('B1', 'TASCLIM_SRES-B1', 1)
        else:
            dest_filename = fname

        zf = zipfile.ZipFile(srcdir)
        yearlist = list(set([os.path.dirname(fp) for fp in zf.namelist()]))
        for year in yearlist: 
            # unpack contains one destination datasets
            base_dir = dest_filename + '_' + year
            ziproot = create_target_dir(base_dir)

            convert(srcdir, ziproot, base_dir, year, fname)
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
