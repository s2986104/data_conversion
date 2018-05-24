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
import time


JSON_TEMPLATE = "climond.template.json"


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


def convert(srcdir, ziproot, basename, filename, year):
    """copy all files and convert if necessary to zip preparation dir.
    """
    # 35 layers
    for layer in range(1, 36):
        if year:
            # Future climate data
            srcfile = '{0}_{1:02d}_{2}.tif'.format(filename, layer, year)
            vsizip_src_dir = "/vsizip/" + os.path.join(srcdir, year, srcfile)
        else:
            # Current climate data
            vsizip_src_dir = "/vsizip/" + os.path.join(srcdir, 'CLIMOND_{0:02d}.tif'.format(layer))
        # just copy all the files
        destfname = 'CLIMOND_{0:02d}.tif'.format(layer)
        destfile = os.path.join(ziproot, basename, 'data', destfname)
        gdal_translate(vsizip_src_dir, destfile)

        # scale B04 by a factor of 100
        if layer == 4:
            scale(destfile, 100.0, ziproot)


def gen_metadatajson(template, ziproot, basename, year):
    """read metadata template and populate rest of fields
    and write to ziproot + '/bccvl/metadata.json'
    """
    md = json.load(open(template, 'r'))
    
    # Update the title, and year of temporal_coverage
    if year is None:
        # Current climate dataset
        year = '1975-2005 (current)'
        start_year = 1961
        md['title'] = md['title'].format(year=year)
    else:
        start_year = int(year) - 14
        md['title'] = md['title'].format(year=year)
    md['temporal_coverage']['start'] = str(start_year)
    md['temporal_coverage']['end'] = str(start_year + 29)
    md['bounding_box'] = {
        "top": "-9.005",
        "right": "153.9950000",
        "bottom": "-43.7450000",
        "left": "112.8950000"
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
        if fname.startswith('CLIMOND_A2'):
            dest_filename = fname.replace('CLIMOND_A2', 'CLIMOND_SRES-A2', 1)
        elif fname.startswith('CLIMOND_A1B'):
            dest_filename = fname.replace('CLIMOND_A1B', 'CLIMOND_SRES-A1B', 1)
        else:
            dest_filename = fname

        tries = 0
        # Make sure file is online
        while True:
            try:
                tries += 1
                zf = zipfile.ZipFile(srcdir)
                print "File {0} is online".format(srcdir)
                break
            except Exception as e:
                if tries > 10:
                    print "Fail to make file {0} online!!".format(srcdir)
                    break
                print "Waiting for file {0} to be online ...".format(srcdir)
                time.sleep(60)

        if dest_filename.startswith('CLIMOND_CURRENT'):
            # Current climate dataset        
            base_dir = dest_filename
            ziproot = create_target_dir(base_dir)

            convert(srcdir, ziproot, base_dir, fname, None)
            gen_metadatajson(JSON_TEMPLATE, ziproot, base_dir, None)
            zipbccvldataset(ziproot, destdir, base_dir)
            if ziproot:
                shutil.rmtree(ziproot)
        else:
            # Future climate dataset
            yearlist = list(set([os.path.dirname(fp) for fp in zf.namelist()]))
            for year in yearlist: 
                # unpack contains one destination datasets
                base_dir = dest_filename + '_' + year
                ziproot = create_target_dir(base_dir)

                convert(srcdir, ziproot, base_dir, fname, year)
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
