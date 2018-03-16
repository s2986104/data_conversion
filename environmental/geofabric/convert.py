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
RDI_ATTRIBUTE_FILE = "stream_attributesv1.1.7.gdb.zip"
NODATA_VALUE = -99999

GEOFABRIC_LAYERS = {
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
GEOFABRIC_ATTRIBUTES = {
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

# BCCVL layer vocabularies
BCCVL_LAYER_TYPES = {
    # climate types
    'catannrad' : 'catmeanradiation',
    'catanntemp' : 'catmeantemp',
    'catcoldmthmin' : 'catcoldmonthmin',
    'cathotmthmax' : 'cathotmonthmax',
    'catannrain' : 'catmeanrain',
    'catdryqrain' : 'catdriestqrain',
    'catwetqrain' : 'catwetestqrain',
    'catwarmqrain' : 'catwarmestqrain',
    'catcoldqrain' : 'catcoldestqrain',
    'catcoldqtemp' : 'catcoldestqtemp',
    'catdryqtemp' : 'catdriestqtemp',
    'catwetqtemp' : 'catwestestqtemp',
    'catanngromega' : 'megathermgrowindex',
    'catanngromeso' : 'mesothermgrowindex',
    'catanngromicro' : 'microthermgrowindex',
    'catgromegaseas' : 'megaseasongrowindex',
    'catgromesoseas' : 'mesoseasongrowindex',
    'catgromicroseas' : 'microseasongrowindex',
    'caterosivity' : 'rainerosivityfactor',
    'suberosivity' : 'rainerosivityfactor',
    'strannrad' : 'strmeanradiation',
    'stranntemp' : 'strmeantemp',
    'strcoldmthmin' : 'strcoldmonthmin',
    'strhotmthmax' : 'strhotmonthmax',
    'strcoldqtemp' : 'strcoldestqtemp',
    'strdryqtemp' : 'strdriestqtemp',
    'strwetqtemp' : 'strwetestqtemp',
    'strannrain' : 'strmeanrain',
    'strdryqrain' : 'strdriestqrain',
    'strwetqrain' : 'strwestestqrain',
    'strwarmqrain' : 'strwarmestqrain',
    'strcoldqrain' : 'strcoldestqrain',
    'stranngromega' : 'megathermgrowindex',
    'stranngromeso' : 'mesothermgrowindex',
    'stranngromicro' : 'microthermgrowindex',
    'strgromegaseas' : 'megaseasongrowindex',
    'strgromesoseas' : 'mesoseasongrowindex',
    'strgromicroseas' : 'microseasongrowindex',
    # substrate types
    'cat_carbnatesed' : 'carbonatesedimentrock',
    'cat_igneous' : 'igneousrock',
    'cat_metamorph' : 'metamorphicrock',
    'cat_oldrock' : 'oldbedrock',
    'cat_othersed' : 'othersedimentrock',
    'cat_sedvolc' : 'mixedsedimentigneousrock',
    'cat_silicsed' : 'siliciclasticrock',
    'cat_unconsoldted' : 'unconsolidatedrock',
    'cat_a_ksat' : 'sathydraulicconductivity',
    'cat_solpawhc' : 'WaterHoldCapacity',
    'str_carbnatesed' : 'carbonatesedimentrock',
    'str_igneous' : 'igneousrock',
    'str_metamorph' : 'metamorphicrock',
    'str_oldrock' : 'oldbedrock',
    'str_othersed' : 'othersedimentrock',
    'str_sedvolc' : 'mixedsedimentigneousrock',
    'str_silicsed' : 'siliciclasticrock',
    'str_unconsoldted' : 'unconsolidatedrock',
    'str_a_ksat' : 'sathydraulicconductivity',
    'str_claya' : 'clayAhorizon',
    'str_clayb' : 'clayBhorizon',
    'str_sanda' : 'sandAhorizon',
    # terrain types
    'catarea' : 'areatotal',
    'catareadiv' : 'areadivided',
    'catelemax' : 'elevationmax',
    'catelemean' : 'elevationmean',
    'catrelief' : 'relief',
    'catslope' : 'slope',
    'catstorage' : 'storage',
    'elongratio' : 'elongationratio',
    'reliefratio' : 'reliefratio',
    'subarea' : '',
    'subelemax' : '',
    'subelemean' : '',
    'subslope' : '',
    'subslope.gt.10' : '',
    'subslope.gt.30' : '',
    'strahler' : 'strahlerorder',
    'strelemax' : 'segelevationmax',
    'strelemean' : 'segelevationmean',
    'strelemin' : 'segelevationmin',
    'valleyslope' : 'segslope',
    'downavgslp' : 'downstreamslope',
    'downmaxslp' : 'downstreamslopemax',
    'upsdist' : 'sourcedistance',
    'd2outlet' : 'outletdistance',
    'aspect' : 'localaspect',
    'confinement' : 'confinement',
    # vegetation types
    'catbare_ext' : 'bareextant',
    'catforests_ext' : 'forestcover',
    'catgrasses_ext' : 'grasscover',
    'catnodata_ext' : 'nodataextant',
    'catwoodlands_ext' : 'woodlandcover',
    'catshrubs_ext' : 'shrubcover',
    'catbare_nat' : 'naturallybare',
    'catforests_nat' : 'natforestcover',
    'catgrasses_nat' : 'natgrasscover',
    'catnodata_nat' : 'natnodata',
    'catshrubs_nat' : 'natshrubcover',
    'catwoodlands_nat' : 'natwoodlandcover',
    'strbare_ext' : 'bareextant',
    'strforests_ext' : 'forestcover',
    'strgrasses_ext' : 'grasscover',
    'strnodata_ext' : 'nodataextant',
    'strwoodlands_ext' : 'woodlandcover',
    'strshrubs_ext' : 'shrubcover',
    'strbare_nat' : 'naturallybare',
    'strforests_nat' : 'natforestcover',
    'strgrasses_nat' : 'natgrasscover',
    'strnodata_nat' : 'natnodata',
    'strwoodlands_nat' : 'natwoodlandcover',
    'strshrubs_nat' : 'natshrubcover',
    # NPP types
    'nppbaseann' : 'npp00', 
    'nppbase1' : 'npp01',   
    'nppbase2' : 'npp02',   
    'nppbase3' : 'npp03',   
    'nppbase4' : 'npp04',   
    'nppbase5' : 'npp05',   
    'nppbase6' : 'npp06',   
    'nppbase7' : 'npp07',   
    'nppbase8' : 'npp08',   
    'nppbase9' : 'npp09',   
    'nppbase10' : 'npp10',  
    'nppbase11' : 'npp11',  
    'nppbase12' : 'npp12',  
    # Landuse types (including population)
    'cat_mod' : 'modifiedland',
    'cat_irr' : 'irrigatedland',
    'cat_aqu' : 'aquaculture',
    'cat_intan' : 'animalproduction',
    'cat_intpl' : 'plantproduction',
    'cat_pest' : 'pestherbicidesused',
    'cat_fert' : 'fertilzerused',
    'cat_forstry' : 'forestryland',
    'cat_mining' : 'miningland',
    'cat_urban' : 'urbanland',
    'cat_drainage' : 'irridrainageland',
    'cat_artimp' : 'artficialimpoundment',
    'cat_road' : 'roadland',
    'sub_mod' : 'modifiedland',
    'sub_irr' : 'irrigatedland',
    'sub_aqu' : 'aquaculture',
    'sub_intan' : 'animalproduction',
    'sub_intpl' : 'plantproduction',
    'sub_pest' : 'pestherbicidesused',
    'sub_fert' : 'fertilzerused',
    'sub_for' : 'forestryland',
    'sub_min' : 'miningland',
    'sub_urb' : 'urbanland',
    'sub_drain' : 'irridrainageland',
    'sub_artimp' : 'artficialimpoundment',
    'sub_road' : 'roadland',
    'str_mod' : 'modifiedland',
    'str_irr' : 'irrigatedland',
    'str_aqu' : 'aquaculture',
    'str_intan' : 'animalproduction',
    'str_intpl' : 'plantproduction',
    'str_pest' : 'pestherbicidesused',
    'str_fert' : 'fertilzerused',
    'str_forstry' : 'forestryland',
    'str_mining' : 'miningland',
    'str_urban' : 'urbanland',
    'str_drainage' : 'irridrainageland',
    'str_artimp' : 'artficialimpoundment',
    'str_road' : 'roadland',
    'catpopmax' : 'popdensitymax',
    'catpopmean' : 'popdensitymean',
    'catpop_gt_1' : 'popdensitygter1',
    'catpop_gt_10' : 'popdensitygter10',
    'subpopmean' : 'popdensitymean',
    'subpopmax' : 'popdensitymax',
    'subpop_gt_1' : 'popdensitygter1',
    'subpop_gt_10' : 'popdensitygter10',
    'strpopmean' : 'popdensitymean',
    'strpopmax' : 'popdensitymax',
    'strpop_gt_1' : 'popdensitygter1',
    'strpop_gt_10' : 'popdensitygter10',
    # RDI types
    'cdi' : 'catdisturbindex',
    'scdi' : 'subcatdisturbindex',
    'sfrdi' : 'segflowdisturbindex',
    'frdi' : 'flowregimedisturbindex',
    'rdi' : 'riverdisturbindex',
    'ef' : 'extindsrcptfactor',
    'luf' : 'landusefactor',
    'lbf' : 'leveebankfactor',
    'nwisf' : 'nwisettlefactor',
    'gdsf' : 'geodatasettlefactor',
    'sdsf' : 'otherdatasettlefactor',
    'sf' : 'maxsettlefactor',
    'nwiif' : 'nwiinffactor',
    'gdif' : 'geodatainffactor',
    'if' : 'maxinffactor',
    'nwiimf' : 'nwiimpfactor',
    'gdimf' : 'geodataimpfactor',
    'imf' : 'maximpfactor',
    'nwifdf' : 'nwidiverfactor',
    'gdfdf' : 'geodatadiverfactor',
    'fdf' : 'maxdiverfactor',
    'sdif' : 'sdif',
    'nwief' : 'nwief',
    'gdef' : 'gdef',
    'sdef' : 'sdef',
    'sdimf' : 'sdimf',
    'sdfdf' : 'sdfdf',
    # Connectivity types
    'conlen' : 'barfreelengmin',
    'conlenres' : 'barfreelengresv',
    'conlendam' : 'barfreelengdam',
    'totlen' : 'totalcatlength',
    'dupreservr' : 'maxupstbffplengres',
    'd2reservor' : 'unrestdowndistres',
    'distupdamw' : 'maxupstbffplengdam',
    'd2damwall' : 'unrestdowndistdam',
    'artfbarier' : 'barrierupdownstr',
    'barrierdown' : 'barrierdownstr',
    'barrierup' : 'barrierupstr',
    'cliffdown' : 'cliffdownstr',
    'cliffup' : 'cliffupstr',
    'waterfall' : 'waterfallflow',
    'wfalldown' : 'waterfallupstr',
    'waterfallup' : 'waterfalldownstr',
    # Network types
    'strdensity' : 'strdensity',
    'lakes' : 'lakeportion',
    'springs' : 'springportion',
    'watcrsarea' : 'watercourseportion',
    'waterholes' : 'waterholeportion',
    'wateryness' : 'waterynessind',
    'rchlen' : 'strsegmentleng',
    'no_waterholes' : 'waterholecount',
    'no_springs' : 'springcount',
    'km_waterholes' : 'waterholedensity',
    'km_springs' : 'springdensity',
    'a_lakes' : 'lakearea',
    'km_lakes' : 'lakedensity',
    'a_wcourse' : 'watercoursearea',
    'km_wcourse' : 'watercoursedensity',
}

def geotif_output_filename(destdir, boundtype, layername, attrname):
    return os.path.join(destdir, "data", "{}_{}_{}.tif".format(boundtype, layername, attrname))

def getDataType(rasterfile):
    rasterLayer = gdal.Open(rasterfile)
    if rasterLayer is None:
        raise Exception('Could not open file {}'.format(rasterfile))
    dtype = rasterLayer.GetRasterBand(1).DataType
    rasterLayer = None
    return dtype

def create_target_dir(destdir, destfile):
    """create zip folder structure in tmp location.
    return root folder
    """
    root = os.path.join(destdir, destfile)
    os.mkdir(root)
    os.mkdir(os.path.join(root, 'data'))
    os.mkdir(os.path.join(root, 'bccvl'))
    return root

def generate_metadatajson(dest, description, boundtype, layername, updatemd=False):
    """read metadata template and populate rest of fields
    and write to dest + '/bccvl/metadata.json'
    """
    md = json.load(open(JSON_TEMPLATE, 'r'))
    lyrname = 'Current Climate' if layername == 'climate' else layername.title()
    md['title'] = 'Freshwater {boundtype} Data (Australia), {layername}, 9 arcsec (~250 m)'.format(layername=lyrname, boundtype=boundtype.title())
    md['description'] = description
    md['genre'] = "Climate" if layername == 'climate' else 'Environmental'

    # Add in the layer information. Unzip existing zip dataset if update metadata only
    if updatemd:
        unzip_dataset(dest, os.path.dirname(dest.strip('/')))

    filesmd = {}
    for attrname in GEOFABRIC_ATTRIBUTES[boundtype].get(layername, {}):
        full_pathname = geotif_output_filename(dest, boundtype, layername, attrname)
        zip_pathname = geotif_output_filename(os.path.basename(dest.strip('/')), boundtype, layername, attrname)
        dtype = getDataType(full_pathname)
        data_type = "continuous"
        if dtype == GDT_Int32 and BCCVL_LAYER_TYPES[attrname] not in ['watercoursearea', 'lakearea', 'springcount', 'waterholecount']:
            data_type = "discrete"

        if BCCVL_LAYER_TYPES[attrname] in ['barrierdownstr', 'barrierupdownstr', 'barrierupstr', \
                'cliffdownstr', 'cliffupstr', 'waterfalldownstr', 'waterfallflow', 'waterfallupstr', 'leveebankfactor']:
            data_type = "discrete"

        filesmd[zip_pathname] = { 
            "layer": BCCVL_LAYER_TYPES[attrname], 
            "data_type": data_type
        }
    md['files'] = filesmd

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

def unzip_dataset(ziproot, dest):
    workdir = os.path.dirname(ziproot)
    zipdir = os.path.basename(ziproot)
    zipname = os.path.abspath(os.path.join(dest, zipdir + '.zip'))
    ret = os.system(
        'cd {0}; unzip -o {1}'.format(workdir, zipname)
    )
    if ret != 0:
        raise Exception("can't unzip {0} ({1})".format(zipname, ret))

def update_dataset_file(ziproot, dest, datapath=os.path.join('bccvl', 'metadata.json')):
    workdir = os.path.dirname(ziproot)
    zipdir = os.path.basename(ziproot)
    zipname = os.path.abspath(os.path.join(dest, zipdir + '.zip'))

    # Replace the specified file of the zip dataset
    print "Updating {0} with {1}".format(ziproot, datapath)
    ret = os.system(
        'cd {0}; zip -m {1} {2}'.format(workdir, zipname, os.path.join(zipdir, datapath))
    )
    if ret != 0:
        raise Exception("can't update {0} with {1} ({2})".format(zipname, datapath, ret))

def get_attribute(attrname, tablename, attrgdbfile):
    # Extract the attribute values from the attribute table
    sqlcmd = "select segmentno, {attrname} from {tablename}".format(attrname=attrname, tablename=tablename)
    attDriver = ogr.GetDriverByName("OpenFileGDB")
    attDataSource = attDriver.Open(attrgdbfile, 0)
    attLayer = attDataSource.ExecuteSQL(sqlcmd)
    valueType = attLayer.GetLayerDefn().GetFieldDefn(1).GetType()   # 0 = integer

    # Check whether to include RAT aux.xml file
    # Loop through to make an attribute dict
    values = {}
    for feature in attLayer:
        segmentno = feature.GetField(0)
        value = feature.GetField(1)
        # Replacing -99 with nodatavalue
        if value == -99.0:
            value = NODATA_VALUE
        values[segmentno] = value
    return (valueType, values)

def extractAsGeotif(rasterLayer, bandData, attrname, tablename, attrgdbfile, outfilename):
    # Get the attribute values and type (i.e. 0 = integer)
    dtype, values = get_attribute(attrname, tablename, attrgdbfile)
    pixel_dtype = GDT_Int32 if dtype == ogr.OFTInteger else GDT_Float32
    value_dtype = numpy.int32 if dtype == ogr.OFTInteger else numpy.float32

    # Create dataset for the layer output
    driver = rasterLayer.GetDriver()
    rows = rasterLayer.RasterYSize
    cols = rasterLayer.RasterXSize

    try:
        outData = None
        outDataset = driver.Create(outfilename, cols, rows, 1, pixel_dtype, ['COMPRESS=LZW', 'TILED=YES'])
        if outDataset is None:
            raise Exception('Could not create {}'.format(outfilename))

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
    parser.add_argument('--updatemd', action='store_true', help='update metadata file')
    params = vars(parser.parse_args(argv[1:]))
    srcdir = params.get('srcdir')
    destdir = params.get('destdir')
    boundtypes = [params.get('type')] if params.get('type') is not None else ['catchment', 'stream']
    table = params.get('table', None)
    updmd = params.get('updatemd', False)
    
    try:
        for boundtype in boundtypes:
            for rasterfile, layername, tablename, description in GEOFABRIC_LAYERS[boundtype]:
                if table and layername != table:
                    continue

                # Create a dataset for each boundary type and associated table
                try:
                    destfile = 'geofabric_{}_{}'.format(boundtype, layername)
                    ziproot = create_target_dir(destdir, destfile)

                    # generating geotif files if update metadata is not speciedied
                    if not updmd:
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
                        for attrname in GEOFABRIC_ATTRIBUTES[boundtype].get(layername, {}):
                            attr_gdbfilename = os.path.join(srcdir, RDI_ATTRIBUTE_FILE if layername == 'rdi' else ATTRIBUTE_FILE)
                            outfilename = geotif_output_filename(ziproot, boundtype, layername, attrname)
                            print("generating {} ...".format(outfilename))
                            extractAsGeotif(rasterLayer, bandData, attrname, tablename, attr_gdbfilename, outfilename)

                        # Close the raster layer
                        rasterLayer = None

                    # generate metada file and zip out/update the dataset
                    generate_metadatajson(ziproot, description, boundtype, layername, updmd)
                    if updmd:
                        # Replace metadata file in the zipped dataset
                        update_dataset_file(ziproot, destdir, os.path.join('bccvl', 'metadata.json'))
                    else:
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