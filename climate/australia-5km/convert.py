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

CURRENT_TEMPLATE = u'Current climate layers for Australia, 2.5arcmin (~5km)'
FUTURE_TEMPLATE = u'Climate Projection {0} based on {1}, 2.5arcmin (~5km) - {2}'
JSON_TEMPLATE = 'bccvl_australia_5km.template.json'

LAYER_MAP = {
    'bioclim_01.tif': 'B01',
    'bioclim_02.tif': 'B02',
    'bioclim_03.tif': 'B03',
    'bioclim_04.tif': 'B04',
    'bioclim_05.tif': 'B05',
    'bioclim_06.tif': 'B06',
    'bioclim_07.tif': 'B07',
    'bioclim_08.tif': 'B08',
    'bioclim_09.tif': 'B09',
    'bioclim_10.tif': 'B10',
    'bioclim_11.tif': 'B11',
    'bioclim_12.tif': 'B12',
    'bioclim_13.tif': 'B13',
    'bioclim_14.tif': 'B14',
    'bioclim_15.tif': 'B15',
    'bioclim_16.tif': 'B16',
    'bioclim_17.tif': 'B17',
    'bioclim_18.tif': 'B18',
    'bioclim_19.tif': 'B19',
}

def ungz(filename):
    """gunzip given filename.
    """
    ret = os.system('gunzip {0}'.format(filename))
    if ret != 0:
        raise Exception("can't gunzip {0} ({1})".format(filename, ret))


def unpack(zipname, path):
    """unpack zipfile to path
    """
    zipf = zipfile.ZipFile(zipname, 'r')
    zipf.extractall(path)


def convert(folder, dest):
    """convert .asc.gz files in folder to .tif in dest
    """
    for srcfile in glob.glob(os.path.join(folder, '*/*.asc.gz')):
        ungz(srcfile)
        srcfile = srcfile[:-len('.gz')]
        basename = os.path.basename(srcfile)
        destfile = os.path.join(dest, 'data',
                                basename[:-len('.asc')]) + '.tif'
        #ret = os.system('gdal_translate -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" {0} {1}'.format(srcfile,
        ret = os.system('gdal_translate -of GTiff {0} {1}'.format(srcfile,
                                                                  destfile))
        if ret != 0:
            raise Exception("can't gdal_translate {0} ({1})".format(srcfile,
                                                                    ret))


def gen_metadatajson(template, dest):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """
    base = os.path.basename(dest)
    # parse info from filename
    # check for future climate dataset:
    md = json.load(open(template, 'r'))
    m = re.match(r'(\w*)_([\w-]*)_(\d*)', base)
    if m:
        md[u'temporal_coverage'][u'start'] = unicode(m.group(3))
        md[u'temporal_coverage'][u'end'] = unicode(m.group(3))
        md[u'emsc'] = unicode(m.group(1))
        md[u'gcm'] = unicode(m.group(2))
        md[u'title'] = FUTURE_TEMPLATE.format(
            md[u'emsc'].upper(),
            md[u'gcm'].upper(),
            md[u'temporal_coverage'][u'end'])
    else:
        # can only be current
        md[u'temporal_coverage'][u'start'] = u'1976'
        md[u'temporal_coverage'][u'end'] = u'2005'
        md[u'title'] = CURRENT_TEMPLATE
    md['files'] = {}
    for filename in glob.glob(os.path.join(dest, '*', '*.tif')):
        filename = filename[len(os.path.dirname(dest)):].lstrip('/')
	md['files'][filename] = {
            'layer': LAYER_MAP[os.path.basename(filename)]
        }
    if 'current' in dest.lower():
        md['genre'] = 'Climate'
    else:
        md['genre'] = 'FutureClimate'
    mdfile = open(os.path.join(dest, 'bccvl', 'metadata.json'), 'w')
    json.dump(md, mdfile, indent=4)
    mdfile.close()


def unzip_dataset(dsfile):
    """unzip source dataset and return unzip location
    """
    tmpdir = tempfile.mkdtemp()
    try:
        unpack(dsfile, tmpdir)
    except:
        shutil.rmtree(tmpdir)
        raise
    return tmpdir


def create_target_dir(destdir, srcfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    basename, _ = os.path.splitext(os.path.basename(srcfile))
    root = os.path.join(destdir, basename)
    os.mkdir(root)
    os.mkdir(os.path.join(root, 'data'))
    os.mkdir(os.path.join(root, 'bccvl'))
    return root


def zipbccvldataset(ziproot, dest):
    workdir = os.path.dirname(ziproot)
    zipdir = os.path.basename(ziproot)
    zipname = os.path.abspath(os.path.join(dest, zipdir + '.zip'))
    ret = os.system('cd {0}; zip -r {1} {2}'.format(workdir,
                                                    zipname,
                                                    zipdir))
    if ret != 0:
        raise Exception("can't zip {0} ({1})".format(ziproot, ret))


def main(argv):
    ziproot = None
    srctmpdir = None
    try:
        if len(argv) != 3:
            print "Usage: {0} <srczip> <destdir>".format(argv[0])
            sys.exit(1)
        srcfile = argv[1]
        dest = argv[2]
        # TODO: check src exists and is zip?
        # TODO: check dest exists
        srctmpdir = unzip_dataset(srcfile)
        # unpack contains one destination datasets
        ziproot = create_target_dir(dest, srcfile)
        convert(srctmpdir, ziproot)
        gen_metadatajson(JSON_TEMPLATE, ziproot)
        zipbccvldataset(ziproot, dest)
    finally:
        # cleanup temp location
        if ziproot:
            shutil.rmtree(ziproot)
        if srctmpdir:
            shutil.rmtree(srctmpdir)

if __name__ == "__main__":
    main(sys.argv)

