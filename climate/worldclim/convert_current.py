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

TMPDIR = os.getenv("BCCVL_TMP", "/mnt/playground/")

JSON_TEMPLATE = 'worldclim.template.json'
TITLE_TEMPLATE = u'WorldClim Current Conditions (1950-2000) at {}'

FILE_MAP = {
    'alt':    'altitude',
    'prec_1': 'prec_01',
    'prec_2': 'prec_02',
    'prec_3': 'prec_03',
    'prec_4': 'prec_04',
    'prec_5': 'prec_05',
    'prec_6': 'prec_06',
    'prec_7': 'prec_07',
    'prec_8': 'prec_08',
    'prec_9': 'prec_09',
    'prec_10': 'prec_10',
    'prec_11': 'prec_11',
    'prec_12': 'prec_12',
    'tmax_1': 'tmax_01',
    'tmax_2': 'tmax_02',
    'tmax_3': 'tmax_03',
    'tmax_4': 'tmax_04',
    'tmax_5': 'tmax_05',
    'tmax_6': 'tmax_06',
    'tmax_7': 'tmax_07',
    'tmax_8': 'tmax_08',
    'tmax_9': 'tmax_09',
    'tmax_10': 'tmax_10',
    'tmax_11': 'tmax_11',
    'tmax_12': 'tmax_12',
    'tmean_1': 'tmean_01',
    'tmean_2': 'tmean_02',
    'tmean_3': 'tmean_03',
    'tmean_4': 'tmean_04',
    'tmean_5': 'tmean_05',
    'tmean_6': 'tmean_06',
    'tmean_7': 'tmean_07',
    'tmean_8': 'tmean_08',
    'tmean_9': 'tmean_09',
    'tmean_10': 'tmean_10',
    'tmean_11': 'tmean_11',
    'tmean_12': 'tmean_12',
    'tmin_1': 'tmin_01',
    'tmin_2': 'tmin_02',
    'tmin_3': 'tmin_03',
    'tmin_4': 'tmin_04',
    'tmin_5': 'tmin_05',
    'tmin_6': 'tmin_06',
    'tmin_7': 'tmin_07',
    'tmin_8': 'tmin_08',
    'tmin_9': 'tmin_09',
    'tmin_10': 'tmin_10',
    'tmin_11': 'tmin_11',
    'tmin_12': 'tmin_12',
    'bio_1': 'bioclim_01',
    'bio_2': 'bioclim_02',
    'bio_3': 'bioclim_03',
    'bio_4': 'bioclim_04',
    'bio_5': 'bioclim_05',
    'bio_6': 'bioclim_06',
    'bio_7': 'bioclim_07',
    'bio_8': 'bioclim_08',
    'bio_9': 'bioclim_09',
    'bio_10': 'bioclim_10',
    'bio_11': 'bioclim_11',
    'bio_12': 'bioclim_12',
    'bio_13': 'bioclim_13',
    'bio_14': 'bioclim_14',
    'bio_15': 'bioclim_15',
    'bio_16': 'bioclim_16',
    'bio_17': 'bioclim_17',
    'bio_18': 'bioclim_18',
    'bio_19': 'bioclim_19',
}

LAYER_MAP = {
    'altitude.tif': 'Altitude',
    'prec_01.tif': 'PR1',
    'prec_02.tif': 'PR2',
    'prec_03.tif': 'PR3',
    'prec_04.tif': 'PR4',
    'prec_05.tif': 'PR5',
    'prec_06.tif': 'PR6',
    'prec_07.tif': 'PR7',
    'prec_08.tif': 'PR8',
    'prec_09.tif': 'PR9',
    'prec_10.tif': 'PR10',
    'prec_11.tif': 'PR11',
    'prec_12.tif': 'PR12',
    'tmax_01.tif': 'TX1',
    'tmax_02.tif': 'TX2',
    'tmax_03.tif': 'TX3',
    'tmax_04.tif': 'TX4',
    'tmax_05.tif': 'TX5',
    'tmax_06.tif': 'TX6',
    'tmax_07.tif': 'TX7',
    'tmax_08.tif': 'TX8',
    'tmax_09.tif': 'TX9',
    'tmax_10.tif': 'TX10',
    'tmax_11.tif': 'TX11',
    'tmax_12.tif': 'TX12',
    'tmean_01.tif': 'TM1',
    'tmean_02.tif': 'TM2',
    'tmean_03.tif': 'TM3',
    'tmean_04.tif': 'TM4',
    'tmean_05.tif': 'TM5',
    'tmean_06.tif': 'TM6',
    'tmean_07.tif': 'TM7',
    'tmean_08.tif': 'TM8',
    'tmean_09.tif': 'TM9',
    'tmean_10.tif': 'TM10',
    'tmean_11.tif': 'TM11',
    'tmean_12.tif': 'TM12',
    'tmin_01.tif': 'TN1',
    'tmin_02.tif': 'TN2',
    'tmin_03.tif': 'TN3',
    'tmin_04.tif': 'TN4',
    'tmin_05.tif': 'TN5',
    'tmin_06.tif': 'TN6',
    'tmin_07.tif': 'TN7',
    'tmin_08.tif': 'TN8',
    'tmin_09.tif': 'TN9',
    'tmin_10.tif': 'TN10',
    'tmin_11.tif': 'TN11',
    'tmin_12.tif': 'TN12',
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

# Layers 1,2 and 5-11 of bioclim are temperature layers
TEMPERATURE_LAYERS =  ['bioclim_{:02d}'.format(x) for x in  range(1,3)+range(5,12)]
# All layers in tmin, tmax, tmean are temperature layers
TEMPERATURE_LAYERS += ["{}_{:02d}".format(x,y) for x in ['tmin','tmax','tmean'] for y in range(1,13)]

RESOLUTION_MAP = {
    '30s': '30 arcsec',
    '2-5m': '2.5 arcmin',
    '5m': '5 arcmin',
    '10m': '10 arcmin',
}

LAYER_TYPE_MAP = {
   'alt': 'alt',
   'tmax': 'tmax',
   'tmin': 'tmin',
   'tmean': 'tmean',      
   'prec': 'prec',
   'bio': 'bioclim',
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
        basename = os.path.basename(srcfile)
        # map filenames to common layer file names
        basename = FILE_MAP[basename]
        destfile = os.path.abspath(os.path.join(dest, 'data', '{}.tif'.format(basename)))
        # Temperature layers get copied to a temp location.
        outfile = os.path.join(tmpdir, '{}.tif'.format(basename)) if basename in TEMPERATURE_LAYERS else destfile
        ret = os.system(
            #'gdal_translate -of GTiff {0} {1}'.format(srcfile, destfile)
            'gdal_translate -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" {0} {1}'.format(
                srcfile,
                outfile)
        )
        if ret != 0:
            raise Exception(
                "can't gdal_translate {0} ({1})".format(
                    srcfile,
                    ret))
        if basename in TEMPERATURE_LAYERS:
            # change temperature representation to C as float to match other
            # datasets.
            print "Changing temperature representation for {}".format(basename)
            cmd = 'gdal_calc.py -A {outfile} --calc="A*0.1" --co="COMPRESS=LZW" --NoDataValue=-9999 --co="TILED=YES" --outfile {destfile} --type "Float32"'.format(
                **locals())
            ret = os.system(cmd)
            if ret != 0:
                raise Exception("COMMAND '{}' failed.".format(cmd))
        else:
            # delete .aux.xml files as they only contain histogram data
            if os.path.exists(destfile + '.aux.xml'):
                os.remove(destfile + '.aux.xml')
    shutil.rmtree(tmpdir)



def gen_metadatajson(template, dest):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """
    base = os.path.basename(dest)
    # parse info from filename
    m = re.match(r'(\w*)_([\w-]*)_(\w*)', base)
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
    src = argv[1]  # TODO: check src exists and is zip?
    dest = argv[2]

    # fail if destination exists but is not a directory
    if os.path.exists(
            os.path.abspath(dest)) and not os.path.isdir(
            os.path.abspath(dest)):
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
        for prefix in LAYER_TYPE_MAP.keys():
            destfile = 'worldclim_{}_{}'.format(res, LAYER_TYPE_MAP[prefix])
            try:
                ziproot = create_target_dir(dest, destfile)
                for srcfile in glob.glob(
                        os.path.join(src, '{}_{}_*'.format(prefix, res))):
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
