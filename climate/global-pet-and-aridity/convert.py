#!/usr/bin/env python
import os
import os.path
import zipfile
import json
import shutil
import sys


TMPDIR = os.getenv("BCCVL_TMP", "/mnt/playground/")


def gen_metadatajson(template, dest):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """
    # parse info from filename
    md = json.load(open(template, 'r'))

    mdfile = open(dest, 'w')
    json.dump(md, mdfile, indent=4)
    mdfile.close()


def main(argv):
    dataroot = None
    if len(argv) != 3:
        print "Usage: {0} <srcdir> <destdir>".format(argv[0])
        sys.exit(1)
    src = argv[1]  # TODO: check src exists and is zip?
    dest = argv[2]

    try:
        # fail if destination exists but is not a directory
        if os.path.exists(
                os.path.abspath(dest)) and not os.path.isdir(
                os.path.abspath(dest)):
            print "Path {} exists and is not a directory.".format(os.path.abspath(dest))
            raise

        # try to create destination if it doesn't exist
        if not os.path.isdir(os.path.abspath(dest)):
            try:
                os.makedirs(os.path.abspath(dest))
            except Exception:
                print "Failed to create directory at {}.".format(os.path.abspath(dest))
            raise

        # extract pet and air to TMPDIR
        with zipfile.ZipFile(os.path.join(src, 'Global PET and Aridity Index.zip'), 'r') as zf:
            zf.extract('Global PET and Aridity Index/Global Aridity - Annual.zip', TMPDIR)
            zf.extract('Global PET and Aridity Index/Global PET - Annual.zip', TMPDIR)
        # unzip data
        with zipfile.ZipFile(os.path.join(TMPDIR, 'Global PET and Aridity Index/Global Aridity - Annual.zip'), 'r') as zf:
            zf.extractall(os.path.join(TMPDIR))
        with zipfile.ZipFile(os.path.join(TMPDIR, 'Global PET and Aridity Index/Global PET - Annual.zip'), 'r') as zf:
            zf.extractall(os.path.join(TMPDIR))
        # create tmp dest
        dataroot = os.path.join(TMPDIR, 'global-pet-and-aridity')
        datadir= os.path.join(dataroot, 'data')
        metadatadir = os.path.join(dataroot, 'bccvl')
        os.makedirs(datadir)
        os.makedirs(metadatadir)
        # convert tif files
        ret = os.system('gdal_translate -stats -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" {0} {1}'.format(
                    os.path.join(TMPDIR, 'PET_he_annual', 'pet_he_yr/'),
                    os.path.join(datadir, 'pet_he_yr.tif')
        ))
        if ret != 0:
            raise Exception(
                "can't gdal_translate")
        ret = os.system('gdal_calc.py -A "{srcfile}" --calc="A*{scale}" --co="COMPRESS=LZW" --NoDataValue=-9999 --co="TILED=YES" --outfile "{destfile}" --type "Float32"'.format(
            scale='0.0001',
            srcfile=os.path.join(TMPDIR, 'AI_Annual', 'ai_yr/'),
            destfile=os.path.join(datadir, 'ai_yr.tif'))
        )
        if ret != 0:
            raise Exception(
                "can't gdal_calc")
        # build metadatafile
        gen_metadatajson('bccvl_metadata_gpeta-template.json', os.path.join(metadatadir, 'metadata.json'))
        # zip result
        ret = os.system(
            'cd {0}; zip -r {1} {2} -x *.aux.xml*'.format(
                TMPDIR,
            'global-pet-and-aridity.zip',
            'global-pet-and-aridity')
        )
        if ret != 0:
            raise Exception("can't zip dataset")
    finally:
        # if dataroot and os.path.exists(dataroot):
        #     shutil.rmtree(dataroot)
        if os.path.exists(os.path.join(TMPDIR, 'AI_annual')):
            shutil.rmtree(os.path.join(TMPDIR, 'AI_annual'))
        if os.path.exists(os.path.join(TMPDIR, 'PET_he_annual')):
            shutil.rmtree(os.path.join(TMPDIR, 'PET_he_annual'))
        if os.path.exists(os.path.join(TMPDIR, 'Global PET and Aridity Index')):
            shutil.rmtree(os.path.join(TMPDIR, 'Global PET and Aridity Index'))

if __name__ == "__main__":
    main(sys.argv)
