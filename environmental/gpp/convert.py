#!/usr/bin/env python
import os
import os.path
import zipfile
import glob
import json
import tempfile
import shutil
import sys
import numpy as np
from osgeo import gdal
from scipy import stats
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
    'gpp_summary_00_07_cov.tif':        ('GPPcov', '2000-2007'),

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


def calc_cov(dsfiles):
    # dsfiles ... list of files to calculate CoV from
    # returns numpy array

    # open files
    datasets = [gdal.Open(fname) for fname in dsfiles]
    # check shape of all datasets:
    shape = set((ds.RasterYSize, ds.RasterXSize) for ds in datasets)
    if len(shape) != 1:
        raise Exception("Raster have different shape")
    ysize, xsize = shape.pop()
    result = np.zeros((ysize, xsize), dtype=np.float32)
    # build buffer array for blocked reading (assume same block size for all datasets, and only one band)
    x_block_size, y_block_size = datasets[0].GetRasterBand(1).GetBlockSize()
    #import pdb; pdb.set_trace()
    for i in range(0, ysize, y_block_size):
        # determine block height to read
        if i + y_block_size < ysize:
            rows = y_block_size
        else:
            rows = ysize - i
        # determine blogk width to read
        for j in range(0, xsize, x_block_size):
            if j + x_block_size < xsize:
                cols = x_block_size
            else:
                cols = xsize - j
        # create buffer array across all datasets
        inarr = np.zeros((rows, cols, len(datasets)), dtype=np.int16)
        for idx, ds in enumerate(datasets):
            inarr[:,:,idx] = ds.GetRasterBand(1).ReadAsArray(xoff=j, yoff=i,
                                                             win_xsize=cols, win_ysize=rows)
        # apply func
        result[i:i+inarr.shape[0], j:j+inarr.shape[1]] = stats.variation(inarr, axis=2)

    return result


def write_array_to_raster(outfile, dataset, template):
    """Write numpy array to raster (geoTIFF format).

    Keyword arguments:
    outfile -- name of the output file
    dataset -- numpy array to be written to file
    template -- path to a gdal dataset to use as template

    Returns: None.
    """
    #log.info("Writing to {}".format(outfile))

    # open template dataset
    templateds = gdal.Open(template)

    # get gtiff driver
    driver = gdal.GetDriverByName('GTiff')

    # create new dataset
    outdata = driver.Create(outfile, xsize=templateds.RasterXSize, ysize=templateds.RasterYSize, bands=1, eType=gdal.GDT_Float32, options=("COMPRESS=LZW", "TILED=YES"))

    # copy over metadata bits
    outdata.SetProjection(templateds.GetProjection())
    outdata.SetGeoTransform(templateds.GetGeoTransform())

    # write data to file
    outdata.GetRasterBand(1).WriteArray(dataset)

    # calculate statistics
    outdata.GetRasterBand(1).ComputeStatistics(False)

    # flush data to disk
    outdata.FlushCache()


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
        # generate cov
        dsfiles = glob.glob(os.path.join(src, '*.[Rr][Ss][Tt]'))
        cov = calc_cov(dsfiles)
        try:
            destfile = 'gpp_summary_00_07'
            ziproot = create_target_dir(dest, destfile)
            write_array_to_raster(os.path.join(ziproot, 'data', destfile + '_cov.tif'), cov, dsfiles[0])
            gen_metadatajson(srcfolder, ziproot)
            zip_dataset(ziproot, dest)
        finally:
            if ziproot:
                shutil.rmtree(ziproot)


if __name__ == "__main__":
    main(sys.argv)
