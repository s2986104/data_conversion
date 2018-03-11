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
import numpy as np

JSON_TEMPLATE = 'bccvl_national-dynamic-land-cover-dataset-2014090101.json'
REDUCED_RAT   = 'bccvl_national-dynamic-land-cover-rat-reduced.tif.aux.xml'

# Dataset layers
DATASET_INFO = {
    'ndlc_DLCDv1_Class': {
       'title': "Australia, Dynamic Land Cover (2000-2008), 9 arcsec (~250 m)",
       'data_type': "categorical",
       'external_url': 'https://data.gov.au/dataset/1556b944-731c-4b7f-a03e-14577c7e68db',
       'fileglob': 'Scene01-DLCDv1_Class.zip'
    },
    'ndlc_trend_evi': {
        'title': 'Australia, Enhanced Vegetation Index (2000-2008), 9 arcsec (~250 m)',
        'data_type': "continuous",
        'external_url': 'https://data.gov.au/dataset/f6951ba7-8968-4f64-9d38-1ed1a25785a0',
        'fileglob': 'Scene01-trend_evi_*.zip'
    }
}

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

TYPE_MAP = {
    ogr.OFTInteger: gdal.GFT_Integer, # 0
    ogr.OFTIntegerList: None, # 1
    ogr.OFTReal: gdal.GFT_Real, # 2
    ogr.OFTRealList: None, # 3
    ogr.OFTString: gdal.GFT_String, # 4
    ogr.OFTStringList: None, # 5
    ogr.OFTWideString: gdal.GFT_String, # 6
    ogr.OFTWideStringList: None, # 7
    ogr.OFTBinary: None,  # 8
    ogr.OFTDate: None, # 9
    ogr.OFTTime: None, # 10
    ogr.OFTDateTime: None, # 11
}


def gen_metadatajson(dsname, src, dest):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """

    # update:
    # data_type: start with captial
    #   files:
    #     <name>:
    #       layer: DLCDv1_Class

    dsinfo = DATASET_INFO[dsname]
    md = json.load(open(src, 'r'))
    md['title'] = dsinfo['title']
    md['data_type'] = dsinfo['data_type']
    md['external_url'] = dsinfo['external_url']
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
        'cd {0}; zip -r {1} {2}'.format(workdir, zipname, zipdir)
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
    # use skip to adjust column index
    layer_defn = mdl.GetLayerDefn()
    for field_idx in range(0, layer_defn.GetFieldCount()):
        field_defn = layer_defn.GetFieldDefn(field_idx)
        field_type = TYPE_MAP[field_defn.GetType()]
        if field_type is None:
            # skip unmappable field type
            continue
        rat.CreateColumn(
            field_defn.GetName(),
            field_type,
            USAGE_MAP[field_defn.GetName()]
        )
    for feature_idx in range(0, mdl.GetFeatureCount()):
        feature = mdl.GetFeature(feature_idx)
        skip = 0
        for field_idx in range (0, feature.GetFieldCount()):
            field_type = TYPE_MAP[feature.GetFieldType(field_idx)]
            if field_type == gdal.GFT_Integer:
                rat.SetValueAsInt(feature_idx, field_idx - skip,
                                  feature.GetFieldAsInteger(field_idx))
            elif field_type == gdal.GFT_Real:
                rat.SetValueAsDouble(feature_idx, field_idx - skip,
                                     feature.GetFieldAsDouble(field_idx))
            elif field_type == gdal.GFT_String:
                rat.SetValueAsString(feature_idx, field_idx - skip,
                                     feature.GetFieldAsString(field_idx))
            else:
                # skip all unmappable field types
                skip += 1
    return rat
def reclassify(tiffname, class_map, destfile):
    driver=gdal.GetDriverByName('GTiff')
    tiffile = gdal.Open(tiffname)
    band = tiffile.GetRasterBand(1)
    data = band.ReadAsArray()

    # reclassification
    for newval, list_vals in class_map.items():
        for i in list_vals:
            data[data==i] = newval

    # create new file
    file2 = driver.Create(destfile, tiffile.RasterXSize , tiffile.RasterYSize , 1)
    file2.GetRasterBand(1).WriteArray(data)
    nodata = band.GetNoDataValue()
    file2.GetRasterBand(1).SetNoDataValue(nodata)

    # spatial ref system
    proj = tiffile.GetProjection()
    georef = tiffile.GetGeoTransform()
    file2.SetProjection(proj)
    file2.SetGeoTransform(georef)
    file2.FlushCache()

    # copy the RAT file for the reduced DLCDv1_Class
    shutil.copy(REDUCED_RAT, destfile + '.aux.xml')


def convert_dataset(srcfolder, dsname):

    # Get the layers from the zip files
    destdir = dsname
    dsglob = DATASET_INFO[dsname].get('fileglob')
    dsttmpdir = tempfile.mkdtemp()
    ziproot = create_target_dir(dsttmpdir, destdir)
    for zipfile in glob.glob(os.path.join(srcfolder, dsglob)):
        try:
            print "converting ", dsname, zipfile
            srctmpdir = unzip_dataset(zipfile)
            
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
                newds.FlushCache()

                # add the reduced classification dataset for DLCDv1_Class dataset
                if tiffile.find('DLCDv1_Class') >= 0:
                    new_tiffile = os.path.splitext(tifbase)[0] + '_Reduced.tif'
                    class_map = {1: range(1,11), 2: range(11,24), 3: range(24,31), 4: range(31,33), 5: range(33,35)}
                    reclassify(tiffile, class_map, os.path.join(ziproot, 'data', new_tiffile))
        except Exception as e:
            print "Error:", e
        finally:
            if srctmpdir:
                shutil.rmtree(srctmpdir)

    # add metadata.json for the dataset
    gen_metadatajson(dsname, JSON_TEMPLATE, ziproot)

    return dsttmpdir

def main(argv):
    srcfolder = 'source/web_data_20140825'
    destfolder = 'bccvl'

    tmpdest = None
    for dsname in DATASET_INFO.keys():
        try:
            tmpdest = convert_dataset(srcfolder, dsname)

            # Remove RAT file for vegetation index dataset
            if dsname == 'ndlc_trend_evi':
                for ratfile in glob.glob(os.path.join(tmpdest, dsname, 'data', '*.tif.aux.xml')):
                    os.remove(ratfile)

            # ziproot = tmpdest/dsname
            zip_dataset(os.path.join(tmpdest, dsname),
                        destfolder)
        finally:
            if tmpdest:
                shutil.rmtree(tmpdest)


if __name__ == "__main__":
    main(sys.argv)
