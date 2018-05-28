#!/usr/bin/env python
import os
import os.path
import glob
import json
import shutil
import sys
import re
import zipfile
import time


JSON_TEMPLATE = "narclim.template.json"
TMPDIR = os.getenv("BCCVL_TMP", "/mnt/workdir/")

def create_target_dir(basename):
    """create zip folder structure in tmp location.
    return root folder
    """
    tmpdir = os.path.join(TMPDIR, "tmp_{}".format(basename))
    os.makedirs(tmpdir)
    os.mkdir(os.path.join(tmpdir, basename))
    os.mkdir(os.path.join(tmpdir, basename, 'data'))
    os.mkdir(os.path.join(tmpdir, basename, 'bccvl'))
    return tmpdir

def read_zipfile(zipname):
    zipf = None
    tries = 0
    # Make sure file is online
    while True:
        try:
            tries += 1
            zipf = zipfile.ZipFile(zipname, 'r')
            print "File {0} is online".format(zipname)
            break
        except Exception as e:
            if tries > 10:
                print "Fail to make file {0} online!!".format(zipname)
                raise Exception("Fail to make file {0} online!!".format(zipname))
            print "Waiting for file {0} to be online ...".format(zipname)
            time.sleep(60)
    return zipf

def scale(src, factor, tmpdir):
    print "scaling {0} by a factor {1}".format(src, factor)
    tmpdest = os.path.join(tmpdir, 'tmpfile_scale.tif')
    cmd = 'gdal_calc.py -A {outfile} --calc="A*{scale}" --co="COMPRESS=LZW" --co="TILED=YES" --outfile {destfile} --type "Float64"'.format(
                outfile=src,
                scale=factor,
                destfile=tmpdest)
    ret = os.system(cmd)
    if ret != 0:
        raise Exception("COMMAND '{}' failed.".format(cmd))

    """Use gdal_translate to recompute statistic"""
    ret = os.system('gdal_translate -of GTiff -stats -co "COMPRESS=LZW" -co "TILED=YES" {0} {1}'
                    .format(tmpdest, src))
    if ret != 0:
        raise Exception("can't gdal_translate to compute statstics {0} ({1})".format(src, ret))

def gdal_translate(src, dest):
    """Use gdal_translate to copy file from src to dest"""
    ret = os.system('gdal_translate -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" {0} {1}'
                    .format(src, dest))
    if ret != 0:
        raise Exception("can't gdal_translate {0} ({1})".format(src, ret))

def convert(srczip, ziproot, basename):
    """copy all files and convert if necessary to zip preparation dir.
    """

    zf = read_zipfile(srczip)
    for filename in zf.namelist():
        vsizip_src_dir = "/vsizip/" + os.path.join(srczip, filename)
        parts = os.path.basename(filename).split('_')
        # just copy all the files
        destfname = 'NARCLIM_{0}'.format(parts[-1])
        destfile = os.path.join(ziproot, basename, 'data', destfname)
        gdal_translate(vsizip_src_dir, destfile)

        # Scale the layer 15
        if parts[-1] == "15":
            scale(destfile, 0.01, ziproot)
    zf.close()


def gen_metadatajson(template, ziproot, basename, year, resolution):
    """read metadata template and populate rest of fields
    and write to ziproot + '/bccvl/metadata.json'
    """
    md = json.load(open(template, 'r'))

    # Update the title, and year of temporal_coverage (20 years period)
    start_year = int(year) - 10
    md['title'] = md['title'].format(year=year, resolution=resolution)
    md['resolution'] = md['resolution'].format(resolution=resolution)
    md['temporal_coverage']['start'] = str(start_year)
    md['temporal_coverage']['end'] = str(start_year + 19)

    # Special case for Aus extent
    if basename.startswith('NaRCLIM_baseline_Aus_Extent'):
        md['bounding_box'] = {
            "top": "-10.0000000",
            "right": "154.0000000",
            "bottom": "-43.7400000",
            "left": "112.9000000"
        }

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

def convert_file(srczip, destdir):
    ziproot = None
    try:
        print "Converting {0} ...".format(srczip)
        fname, ext = os.path.splitext(os.path.basename(srczip))

        # Replace the short emsc with full emsc name
        nameparts = fname.split('_')
        if fname.startswith('NaRCLIM_projected_'):
            resolution = '36 arcsec (1km)'
            year = int(nameparts[2])
            gcm = nameparts[3]
            rcm = nameparts[4]
            dest_filename = 'NaRCLIM_{gcm}_{rcm}_{year}'.format(gcm=gcm, rcm=rcm, year=year)
        elif fname.startswith('NaRCLIM_baseline_'):
            resolution = '36 arcsec (1km)'
            dest_filename = fname
            year = 2000
        elif fname.startswith('NaRCLIM_baseline'):
            resolution = '9 arcsec (250m)'
            dest_filename = fname
            year = 2000
        elif fname.startswith('NaRCLIM_'):
            resolution = '9 arcsec (250m)'
            year = int(nameparts[1])
            gcm = nameparts[2]
            rcm = nameparts[3]
            dest_filename = 'NaRCLIM_{gcm}_{rcm}_{year}'.format(gcm=gcm, rcm=rcm, year=year)
        else:
            raise Exception("Unexpected file {}".format(srczip))

        base_dir = dest_filename
        ziproot = create_target_dir(base_dir)

        convert(srczip, ziproot, base_dir)
        gen_metadatajson(JSON_TEMPLATE, ziproot, base_dir, year, resolution)
        zipbccvldataset(ziproot, destdir, base_dir)
        if ziproot:
            shutil.rmtree(ziproot)
    finally:
        # cleanup temp location
        if ziproot and os.path.exists(ziproot):
            shutil.rmtree(ziproot)


def main(argv):
    srcdir = None
    if len(argv) != 3:
        print "Usage: {0} <srcdir> <destdir>".format(argv[0])
        sys.exit(1)
    srcdir = argv[1]
    destdir = argv[2]


    if os.path.isdir(srcdir):
        for srczip in glob.glob(os.path.join(srcdir, '*.zip')):
            convert_file(srczip, destdir)
    elif os.path.isfile(srcdir):
        convert_file(srcdir, destdir)
    else:
        print "Source {0} does not exist".format(srcdir)
        sys.exit(1)

if __name__ == '__main__':
    main(sys.argv)
