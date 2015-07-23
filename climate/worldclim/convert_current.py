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

TMPDIR = "/mnt/playground/"

JSON_TEMPLATE = 'worldclim.template.json'
TITLE_TEMPLATE = u'WorldClim Current Conditions (1950-2000) at {}'

LAYER_MAP = {
    'alt.tif': 'Altitude',
    'prec_1.tif': 'PR1',
    'prec_2.tif': 'PR2',
    'prec_3.tif': 'PR3',
    'prec_4.tif': 'PR4',
    'prec_5.tif': 'PR5',
    'prec_6.tif': 'PR6',
    'prec_7.tif': 'PR7',
    'prec_8.tif': 'PR8',
    'prec_9.tif': 'PR9',
    'prec_10.tif': 'PR10',
    'prec_11.tif': 'PR11',
    'prec_12.tif': 'PR12',
    'tmax_1.tif': 'TX1',
    'tmax_2.tif': 'TX2',
    'tmax_3.tif': 'TX3',
    'tmax_4.tif': 'TX4',
    'tmax_5.tif': 'TX5',
    'tmax_6.tif': 'TX6',
    'tmax_7.tif': 'TX7',
    'tmax_8.tif': 'TX8',
    'tmax_9.tif': 'TX9',
    'tmax_10.tif': 'TX10',
    'tmax_11.tif': 'TX11',
    'tmax_12.tif': 'TX12',
    'tmean_1.tif': 'TM1',
    'tmean_2.tif': 'TM2',
    'tmean_3.tif': 'TM3',
    'tmean_4.tif': 'TM4',
    'tmean_5.tif': 'TM5',
    'tmean_6.tif': 'TM6',
    'tmean_7.tif': 'TM7',
    'tmean_8.tif': 'TM8',
    'tmean_9.tif': 'TM9',
    'tmean_10.tif': 'TM10',
    'tmean_11.tif': 'TM11',
    'tmean_12.tif': 'TM12',
    'tmin_1.tif': 'TN1',
    'tmin_2.tif': 'TN2',
    'tmin_3.tif': 'TN3',
    'tmin_4.tif': 'TN4',
    'tmin_5.tif': 'TN5',
    'tmin_6.tif': 'TN6',
    'tmin_7.tif': 'TN7',
    'tmin_8.tif': 'TN8',
    'tmin_9.tif': 'TN9',
    'tmin_10.tif': 'TN10',
    'tmin_11.tif': 'TN11',
    'tmin_12.tif': 'TN12',
    'bio_1.tif': 'B01',
    'bio_2.tif': 'B02',
    'bio_3.tif': 'B03',
    'bio_4.tif': 'B04',
    'bio_5.tif': 'B05',
    'bio_6.tif': 'B06',
    'bio_7.tif': 'B07',
    'bio_8.tif': 'B08',
    'bio_9.tif': 'B09',
    'bio_10.tif': 'B10',
    'bio_11.tif': 'B11',
    'bio_12.tif': 'B12',
    'bio_13.tif': 'B13',
    'bio_14.tif': 'B14',
    'bio_15.tif': 'B15',
    'bio_16.tif': 'B16',
    'bio_17.tif': 'B17',
    'bio_18.tif': 'B18',
    'bio_19.tif': 'B19',
}

# Layers 1,2 and 5-11 of bioclim contain temperature in C *10 as integers. 
TEMPERATURE_LAYERS = map(lambda x: 'bio_{}'.format(x), range(1,3)+range(5,12))

RESOLUTION_MAP = {
    '30s': '30 arcsec',
    '2-5m': '2.5 arcmin',
    '5m': '5 arcmin',
    '10m': '10 arcmin',
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


def convert(filename, folder, dest):
    """convert .asc.gz files in folder to .tif in dest
    """
    tmpdir = tempfile.mkdtemp(dir=TMPDIR)    
    # parse info from filename
    base = os.path.basename(filename)
    m = re.match(r'(\w*)_([\w-]*)_(\d*)', base)
    layer = m.group(1)
    for srcfile in glob.glob(os.path.join(folder, '{0}/{0}*'.format(layer))):
        print "DEBUG: srcfile: {}".format(srcfile)
        basename = os.path.basename(srcfile)
        destfile = os.path.join(dest, 'data', '{}.tif'.format(basename))
        # Temperature layers get copied to a temp location. 
        outfile  = os.path.join(tmpdir, '{}.tif'.format(basename)) if basename in TEMPERATURE_LAYERS else destfile
        ret = os.system(
            #'gdal_translate -of GTiff {0} {1}'.format(srcfile, destfile)
            'gdal_translate -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" {0} {1}'.format(srcfile, outfile)
        )
        if ret != 0:
            raise Exception("can't gdal_translate {0} ({1})".format(srcfile, ret))
        if basename in TEMPERATURE_LAYERS:
            # change temperature representation to C as float to match other datasets.
            print "Changing temperature representation for {}".format(basename)
            command = 'gdal_calc.py -A {0} --calc="A*0.1" --creation-option="COMPRESS=LZW" --creation-option="TILED=YES" --outfile {1} --type "Float32"'.format(outfile, destfile)
            ret = os.system(command)
            if ret != 0:
                raise Exception("COMMAND '{}' failed.".format(command))
    shutil.rmtree(tmpdir)



def gen_metadatajson(template, dest):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """
    base = os.path.basename(dest)
    # parse info from filename
    m = re.match(r'(\w*)_([\w-]*)', base)
    # check for future climate dataset:
    md = json.load(open(template, 'r'))
    md[u'title'] = TITLE_TEMPLATE.format(RESOLUTION_MAP[m.group(2)])   
    md[u'temporal_coverage'][u'start'] = u'1950'
    md[u'temporal_coverage'][u'end'] = u'2000'
    md[u'genre'] = u'Climate'
    md[u'resolution'] = RESOLUTION_MAP[m.group(2)]

    md[u'files'] = {}
    for filename in glob.glob(os.path.join(dest, '*', '*.tif')):
        filename = filename[len(os.path.dirname(dest)):].lstrip('/')
        md[u'files'][filename] = {
            u'layer': LAYER_MAP[os.path.basename(filename)]
        }

    mdfile = open(os.path.join(dest, 'bccvl', 'metadata.json'), 'w')
    json.dump(md, mdfile, indent=4)
    mdfile.close()


def unzip_dataset(dsfile):
    """unzip source dataset and return unzip location
    """
    tmpdir = tempfile.mkdtemp(dir=TMPDIR)
    try:
        unpack(dsfile, tmpdir)
    except:
        shutil.rmtree(tmpdir)
        raise
    return tmpdir


def create_target_dir(destdir, destfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    root = os.path.join(destdir, destfile)
    # TODO: make sure there is no stray "root" folder left, shou probably delete it here if it alreayd exists
    os.mkdir(root)
    os.mkdir(os.path.join(root, 'data'))
    os.mkdir(os.path.join(root, 'bccvl'))
    return root


def zipbccvldataset(ziproot, dest):
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
    src  = argv[1] # TODO: check src exists and is zip?
    dest = argv[2] 

    # fail if destination exists but is not a directory
    if os.path.exists(os.path.abspath(dest)) and not os.path.isdir(os.path.abspath(dest)):
        print "Path {} exists and is not a directory.".format(os.path.abspath(dest))
        sys.exit(os.EX_IOERR)

    # try to create destination if it doesn't exist
    if not os.path.isdir(os.path.abspath(dest)):
        try:
            os.makedirs(os.path.abspath(dest))
        except Exception as e:
            print "Failed to create directory at {}.".format(os.path.abspath(dest))
            sys.exit(os.EX_IOERR)

    for res in sorted(RESOLUTION_MAP.keys()):
        # sorting isn't important, it just forces it to 
        # hit the smallest dataset first for testing
        destfile = 'worldclim_{}'.format(res)
        try:
            ziproot = create_target_dir(dest, destfile)
            for srcfile in glob.glob(os.path.join(src, 'bio*_{}_*'.format(res))):
                srctmpdir = unzip_dataset(srcfile)
                convert(srcfile, srctmpdir, ziproot)
                if srctmpdir:
                    shutil.rmtree(srctmpdir)
            gen_metadatajson(JSON_TEMPLATE, ziproot)
            zipbccvldataset(ziproot, dest)
        finally:
            # cleanup temp location
            if ziproot:
                shutil.rmtree(ziproot)

if __name__ == "__main__":
    main(sys.argv)

