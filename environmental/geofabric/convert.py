#!/usr/bin/env python
import os, sys
import os.path
import glob
import json
import tempfile
import shutil
import sys
import re
import zipfile
import argparse
import numpy
import traceback
from osgeo import gdal, ogr
from osgeo.gdalconst import *


JSON_TEMPLATE = "geofabric.template.json"
CATCHMENT_RASTER = 'NationalCatchmentBoundariesRaster1.tif'
STREAM_RASTER = 'DEMDerivedStreamsRaster1.tif'
ATTRIBUTE_FILE = "stream_attributesv1.1.5.gdb.zip"
NODATA_VALUE = -99999

layers = {
    'catchment': [(CATCHMENT_RASTER, 'climate', 'climate_lut', u'Climate Data from 9" DEM of Australia version 3 (2008), ANUCLIM (Fenner School)'), 
                  (CATCHMENT_RASTER, 'vegetation', 'veg_lut', u'NVIS Major Vegetation sub-groups version 3.1'),
                  (CATCHMENT_RASTER, 'substrate', 'substrate_lut', u'Surface geology of Australia 1:1M'), 
                  (CATCHMENT_RASTER, 'terrain', 'terrain_lut', u'Terrain Data from 9" DEM of Australia version 3 (2008)'),
                  (CATCHMENT_RASTER, 'landuse', 'landuse_lut', u'Catchment Scale Land Use Mapping for Australia Update (CLUM Update 04/09)'),
                  (CATCHMENT_RASTER, 'population', 'landuse_lut', u'ABS Population density within 2006 Australian Standard Geographic Classification census collector districts'),
                  (CATCHMENT_RASTER, 'npp', 'npp_lut', u'Net Primary Production (pre-1788)'),
                  (CATCHMENT_RASTER, 'rdi', 'rdi_geodata2_lut', u'River Disturbance Indeces and Factors')],
    'stream':    [(STREAM_RASTER, 'climate', 'climate_lut', u'Climate Data from 9" DEM of Australia version 3 (2008), ANUCLIM (Fenner School)'), 
                  (STREAM_RASTER, 'vegetation', 'veg_lut', u'NVIS Major Vegetation sub-groups version 3.1'),
                  (STREAM_RASTER, 'substrate', 'substrate_lut', u'Surface geology of Australia 1:1M'), 
                  (STREAM_RASTER, 'terrain', 'terrain_lut', u'Terrain Data from 9" DEM of Australia version 3 (2008)'),
                  (STREAM_RASTER, 'landuse', 'landuse_lut', u'Catchment Scale Land Use Mapping for Australia Update (CLUM Update 04/09)'),
                  (STREAM_RASTER, 'population', 'landuse_lut', u'ABS Population density within 2006 Australian Standard Geographic Classification census collector districts'),
                  (STREAM_RASTER, 'network', 'network_lut', u'Stream Network from AusHydro version 1.1.6'),
                  (STREAM_RASTER, 'connectivity', 'connectivity_lut', u'Stream Connectivity from AusHydro version 1.1.6')]
}

# Attributes for dataset
attributes = {
    'catchment': {
        'climate': ['catannrad', 'catanntemp', 'catcoldmthmin', 'cathotmthmax', 'catannrain', 'catdryqrain', 
                    'catwetqrain', 'catwarmqrain', 'catcoldqrain', 'catcoldqtemp', 'catdryqtemp', 'catwetqtemp',
                    'catanngromega', 'catanngromeso', 'catanngromicro', 'catgromegaseas', 'catgromesoseas', 
                    'catgromicroseas', 'caterosivity'],
        'vegetation': ['catbare_ext', 'catforests_ext', 'catgrasses_ext', 'catnodata_ext', 'catwoodlands_ext', 
                       'catshrubs_ext', 'catbare_nat', 'catforests_nat', 'catgrasses_nat', 'catnodata_nat', 
                       'catwoodlands_nat', 'catshrubs_nat'],
        'substrate': ['cat_carbnatesed', 'cat_igneous', 'cat_metamorph', 'cat_oldrock', 'cat_othersed', 
                      'cat_sedvolc', 'cat_silicsed', 'cat_unconsoldted', 'cat_a_ksat', 'cat_solpawhc'],
        'terrain': ['catarea', 'catelemax', 'catelemean', 'catrelief', 'catslope', 'catstorage',
                    'elongratio', 'reliefratio'],
        'npp': ['nppbaseann', 'nppbase1', 'nppbase2', 'nppbase3', 'nppbase4', 'nppbase5', 'nppbase6', 
                'nppbase7', 'nppbase8', 'nppbase9', 'nppbase10', 'nppbase11', 'nppbase12'],
        'landuse': ['cat_aqu', 'cat_artimp', 'cat_drainage', 'cat_fert', 'cat_forstry', 'cat_intan', 'cat_intpl', 
                    'cat_irr', 'cat_mining', 'cat_mod', 'cat_pest', 'cat_road', 'cat_urban'],
        'population': ['catpop_gt_1', 'catpop_gt_10', 'catpopmax', 'catpopmean'],
        'rdi': ['sfrdi','imf', 'fdf', 'scdi', 'ef', 'if', 'sf', 'nwisf', 'gdsf', 'sdsf',
                'nwiif', 'gdif', 'sdif', 'nwief', 'gdef', 'sdef', 'luf', 'lbf', 'nwiimf',
                'gdimf', 'sdimf', 'nwifdf', 'gdfdf', 'sdfdf', 'cdi', 'frdi', 'rdi']
    }, 
    'stream': {
        'climate': ['strannrad', 'stranntemp', 'strcoldmthmin', 'strhotmthmax', 'strannrain', 'strdryqrain', 
                    'strwetqrain', 'strwarmqrain', 'strcoldqrain', 'strcoldqtemp', 'strdryqtemp', 'strwetqtemp',
                    'stranngromega', 'stranngromeso', 'stranngromicro', 'strgromegaseas', 'strgromesoseas', 
                    'strgromicroseas', 'suberosivity'],
        'vegetation': ['strbare_ext', 'strforests_ext', 'strgrasses_ext', 'strnodata_ext', 'strwoodlands_ext', 
                       'strshrubs_ext', 'strbare_nat', 'strforests_nat', 'strgrasses_nat', 'strnodata_nat', 
                       'strwoodlands_nat', 'strshrubs_nat'],
        'substrate': ['str_carbnatesed', 'str_igneous', 'str_metamorph', 'str_oldrock', 'str_othersed', 
                      'str_sedvolc', 'str_silicsed', 'str_unconsoldted', 'str_a_ksat', 'str_sanda',
                      'str_claya', 'str_clayb'],
        'terrain': ['strahler', 'strelemax', 'strelemean', 'strelemin', 'valleyslope', 'downavgslp',
                    'downmaxslp', 'upsdist', 'd2outlet', 'aspect', 'confinement'],
        'landuse': ['str_aqu', 'str_artimp', 'str_drainage', 'str_fert', 'str_forstry', 'str_intan', 'str_intpl',
                    'str_irr', 'str_mining', 'str_mod', 'str_pest', 'str_road', 'str_urban'], 
        'population': ['strpop_gt_1', 'strpop_gt_10', 'strpopmax', 'strpopmean'],
        'network': ['strdensity', 'no_waterholes', 'km_waterholes', 'no_springs', 'km_springs', 'a_lakes',
                    'km_lakes', 'a_wcourse', 'km_wcourse', 'lakes', 'springs', 'watcrsarea', 'waterholes',
                    'wateryness', 'rchlen'],
        'connectivity': ['conlen', 'dupreservr', 'd2reservor', 'barrierdown', 'barrierup', 'distupdamw', 'd2damwall', 
                         'conlenres', 'conlendam', 'artfbarier', 'totlen', 'cliffdown', 'cliffup', 'waterfall',
                         'wfalldown', 'waterfallup']        
    }
}


def create_target_dir(destdir, destfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    root = os.path.join(destdir, destfile)
    os.mkdir(root)
    os.mkdir(os.path.join(root, 'data'))
    os.mkdir(os.path.join(root, 'bccvl'))
    return root

def update_metadatajson(dest, description, boundtype, layername):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """
    md = json.load(open(JSON_TEMPLATE, 'r'))
    lyrname = 'Current Climate' if layername == 'climate' else layername.title()
    md['title'] = 'Geofabric Australia, {layername} dataset ({boundtype}), (2008), 9 arcsec (250 m)'.format(layername=lyrname, boundtype=boundtype)
    md['descriptions'] = description
    md['genre'] = "Climate" if layername == 'climate' else 'Environmental'
    mdfile = open(os.path.join(dest, 'bccvl', 'metadata.json'), 'w')
    json.dump(md, mdfile, indent=4)
    mdfile.close()

def zip_dataset(ziproot, dest):
    workdir = os.path.dirname(ziproot)
    zipdir = os.path.basename(ziproot)
    zipname = os.path.abspath(os.path.join(dest, zipdir + '.zip'))
    ret = os.system(
        'cd {0}; zip -r {1} {2}'.format(workdir, zipname, zipdir)
    )
    if ret != 0:
        raise Exception("can't zip {0} ({1})".format(ziproot, ret))

def get_attribute(attrname, tablename, attrgdbfile):
    # Extract the attribute values from the attribute table
    sqlcmd = "select segmentno, {attrname} from {tablename}".format(attrname=attrname, tablename=tablename)
    attDriver = ogr.GetDriverByName("OpenFileGDB")
    attDataSource = attDriver.Open(attrgdbfile, 0)
    attLayer = attDataSource.ExecuteSQL(sqlcmd)
    valueType = attLayer.GetLayerDefn().GetFieldDefn(1).GetType()   # 0 = integer

    # Loop through to make an attribute dict
    values = {}
    for feature in attLayer:
        segmentno = feature.GetField(0)
        value = feature.GetField(1)
        values[segmentno] = value
    return (valueType, values)

def extractAsGeotif(rasterLayer, bandData, attrname, tablename, attrgdbfile, outfilename):
    # Get the attribute values and type (i.e. 0 = integer)
    dtype, values = get_attribute(attrname, tablename, attrgdbfile)
    pixel_dtype = GDT_Int32 if dtype == 0 else GDT_Float32
    value_dtype = numpy.int32 if dtype == 0 else numpy.float32

    # Create dataset for the layer output
    driver = rasterLayer.GetDriver()
    rows = rasterLayer.RasterYSize
    cols = rasterLayer.RasterXSize

    try:
        outData = None
        outDataset = driver.Create(outfilename, cols, rows, 1, pixel_dtype, ['COMPRESS=LZW', 'TILED=YES'])
        if outDataset is None:
            raise Exception('Could not create {}'.format(outfilename))

        #outData = numpy.full((rows, cols), NODATA_VALUE, dtype=value_dtype)
        mapfunc = numpy.vectorize(values.get, otypes=[value_dtype])
        outData = mapfunc(bandData, NODATA_VALUE)
        del values

        # write the data
        outBand = outDataset.GetRasterBand(1)
        outBand.WriteArray(outData, 0, 0)

        # flush data to disk, set the NoData value and calculate stats
        outBand.FlushCache()
        outBand.SetNoDataValue(NODATA_VALUE)

        # georeference the image and set the projection
        outDataset.SetGeoTransform(rasterLayer.GetGeoTransform())
        outDataset.SetProjection(rasterLayer.GetProjection())
    finally:
        # Release dataset
        if outDataset:
            outDataset = None
        if outData is not None:
            del outData


def main(argv):
    ziproot = None
    srcdir = None
    parser = argparse.ArgumentParser(description='Convert Geofabric datasets in gdb to geotif format')
    parser.add_argument('srcdir', type=str, help='source directory')
    parser.add_argument('destdir', type=str, help='output directory')
    parser.add_argument('--type', type=str, choices=['catchment', 'stream'], help='boundary type')
    parser.add_argument('--table', type=str, help='table name i.e. climate')
    params = vars(parser.parse_args(argv[1:]))
    srcdir = params.get('srcdir')
    destdir = params.get('destdir')
    boundtypes = [params.get('type')] if params.get('type') is not None else ['catchment', 'stream']
    table = params.get('table', None)

    try:
        for boundtype in boundtypes:
            for rasterfile, layername, tablename, description in layers[boundtype]:
                if table and layername != table:
                    continue

                # Create a dataset for each boundary type and associated table
                try:
                    destfile = 'geofabric_{}_{}'.format(boundtype, layername)
                    ziproot = create_target_dir(destdir, destfile)

                    # Open the catchmen/stream boundary raster layer
                    rasterLayer = gdal.Open(os.path.join(srcdir, rasterfile))
                    if rasterLayer is None:
                        raise Exception('Could not open file {}'.format(rasterfile))

                    # read in the band data and get info about it
                    band1 = rasterLayer.GetRasterBand(1)
                    rows = rasterLayer.RasterYSize
                    cols = rasterLayer.RasterXSize
                    bandData = band1.ReadAsArray(0, 0, cols, rows)

                    # For each attribute in the table, create a layer geotif file 
                    for attrname in attributes[boundtype].get(layername, {}):
                        attr_gdbfilename = os.path.join(srcdir, ATTRIBUTE_FILE)
                        outfilename = os.path.join(ziproot, "data", "{}_{}_{}.tif".format(boundtype, layername, attrname))
                        print("generating {} ...".format(outfilename))
                        extractAsGeotif(rasterLayer, bandData, attrname, tablename, attr_gdbfilename, outfilename)

                    # Update metada file and zip out the dataset
                    update_metadatajson(ziproot, description, boundtype, layername)
                    zip_dataset(ziproot, destdir)
                finally:
                    # delete temp directory for the dataset
                    if ziproot and os.path.exists(ziproot):
                        shutil.rmtree(ziproot)
    except Exception as e:
        traceback.print_exc()
        print "Fail to convert: ", e

if __name__ == '__main__':
    main(sys.argv)