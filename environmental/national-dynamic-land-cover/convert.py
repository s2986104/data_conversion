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
from osgeo import gdal, ogr

JSON_TEMPLATE = 'bccvl_national-dynamic-land-cover-dataset-2014090101.json'

# this column map has been handcrafted,
# info can be read from source file with ogrinfo -a l DLCDv1_Class.tif.vat.dbf
USAGE_MAP = {
    'VALUE': gdal.GFU_MinMax,
    'COUNT': gdal.GFU_PixelCount,
    'RED': gdal.GFU_Red,
    'GREEN': gdal.GFU_Green,
    'BLUE': gdal.GFU_Blue,
    'ISO_CLASS': gdal.GFU_Name,
    'CLASSLABEL': gdal.GFU_Generic
}


def gen_metadatajson(src, dest):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """

    # update:
    # data_type: start with captial
    #   files:
    #     <name>:
    #       layer: DLCDv1_Class

    md = json.load(open(src, 'r'))
    md[u'files'] = {}
    for filename in glob.glob(os.path.join(dest, '*', '*.tif')):
        base = os.path.basename(filename)
        base, _ = os.path.splitext(base)

        layer_id = base
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


def unpack(zipname, path):
    """unpack zipfile to path
    """
    zipf = zipfile.ZipFile(zipname, 'r')
    zipf.extractall(path)


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


def get_rat_from_vat(filename):
    md = ogr.Open(filename)
    mdl = md.GetLayer(0)
    # get column definitions:
    rat = gdal.RasterAttributeTable()
    layer_defn = mdl.GetLayerDefn()
    for field_idx in range(0, layer_defn.GetFieldCount()):
        field_defn = layer_defn.GetFieldDefn(field_idx)
        rat.CreateColumn(
            field_defn.GetName(),
            field_defn.GetType(),
            USAGE_MAP[field_defn.GetName()]
        )
    for feature_idx in range(0, mdl.GetFeatureCount()):
        feature = mdl.GetFeature(feature_idx)
        for field_idx in range (0, feature.GetFieldCount()):
            field_type = feature.GetFieldType(field_idx)
            if field_type == gdal.GFT_Integer:
                rat.SetValueAsInt(feature_idx, field_idx,
                                  feature.GetFieldAsInteger(field_idx))
            elif field_type == gdal.GFT_Real:
                rat.SetValueAsDouble(feature_idx, field_idx,
                                     feature.GetFieldAsDouble(field_idx))
            else:
                rat.SetValueAsString(feature_idx, field_idx,
                                     feature.GetFieldAsString(field_idx))
    return rat


def convert_dataset(zipfile, destdir):
    srctmpdir = dsttmpdir = ziproot = None
    try:
        srctmpdir = unzip_dataset(zipfile)
        dsttmpdir = tempfile.mkdtemp()
        ziproot = create_target_dir(dsttmpdir, destdir)
        # find all tif files in srctmpdir:
        for tiffile in glob.glob(os.path.join(srctmpdir, '*.tif')):
            # do we have an associated .vat.dbf?
            ratfile = '{}.vat.dbf'.format(tiffile)
            rat = None
            if os.path.exists(ratfile):
                rat = get_rat_from_vat(ratfile)
            # open dataset
            ds = gdal.Open(tiffile)
            # create new dataset in ziproot/data
            driver = ds.GetDriver()
            tifbase = os.path.basename(tiffile)
            newds = driver.CreateCopy(os.path.join(ziproot, 'data', tifbase),
                                      ds, strict=0,
                                      options=['TILED=YES',
                                               'COMPRESS=LZW',
                                               'PROFILE=GDALGeoTIFF'
                                               ]
            )
            # generate band stats
            band = newds.GetRasterBand(1)
            band.ComputeStatistics(False)
            # attach RAT if we have one
            if rat is not None:
                band.SetDefaultRAT(rat)
            # add metadata.json
            gen_metadatajson(JSON_TEMPLATE, ziproot)
    except Exception as e:
        print "Error:", e
    finally:
        if srctmpdir:
            shutil.rmtree(srctmpdir)
    return dsttmpdir


def main(argv):
    srcfolder = 'source/web_data_20140825'
    destfolder = 'bccvl'

    for dataset in glob.glob(os.path.join(srcfolder, '*.zip')):
        tmpdest = None
        try:
            basename, _ = os.path.splitext(dataset)
            _, basename = basename.split('-')

            tmpdest = convert_dataset(dataset, basename)
            # ziproot = temdest/basename
            zip_dataset(os.path.join(tmpdest, basename),
                        destfolder)
        finally:
            if tmpdest:
                #shutil.rmtree(tmpdest)
                pass


if __name__ == "__main__":
    main(sys.argv)
