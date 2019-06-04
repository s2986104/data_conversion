#!/usr/bin/env python
import os
import os.path
import tempfile
import shutil
import numpy
from concurrent import futures
import copy
from tqdm import tqdm

from osgeo import gdal, ogr, gdal_array
from osgeo.gdalconst import *
from datetime import datetime

from data_conversion.converter import BaseConverter, run_gdal


class GeofabricConverter(BaseConverter):
    CATCHMENT_RASTER = 'NationalCatchmentBoundariesRaster1.tif'
    STREAM_RASTER = 'DEMDerivedStreamsRaster1.tif'
    ATTRIBUTE_FILE = "stream_attributesv1.1.5.gdb.zip"
    RDI_ATTRIBUTE_FILE = "stream_attributesv1.1.7.gdb.zip"
    NODATA_VALUE = -99999

    GEOFABRIC_TABLES = {
        'catchment': [
              ('climate', 'climate_lut'),
              ('vegetation', 'veg_lut'),
              ('substrate', 'substrate_lut'),
              ('terrain', 'terrain_lut'),
              ('landuse', 'landuse_lut'),
              ('population', 'landuse_lut'),
              ('npp', 'npp_lut'),
              ('rdi', 'rdi_geodata2_lut')
        ],
        'stream': [
              ('climate', 'climate_lut'),
              ('vegetation', 'veg_lut'),
              ('substrate', 'substrate_lut'),
              ('terrain', 'terrain_lut'),
              ('landuse', 'landuse_lut'),
              ('population', 'landuse_lut'),
              ('network', 'network_lut'),
              ('connectivity', 'connectivity_lut')
        ]
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
        'catannrad' : 'bioclim_20',
        'catanntemp' : 'bioclim_01',
        'catcoldmthmin' : 'bioclim_06',
        'cathotmthmax' : 'bioclim_05',
        'catannrain' : 'bioclim_12',
        'catdryqrain' : 'bioclim_17',
        'catwetqrain' : 'bioclim_16',
        'catwarmqrain' : 'bioclim_18',
        'catcoldqrain' : 'bioclim_19',
        'catcoldqtemp' : 'bioclim_11',
        'catdryqtemp' : 'bioclim_09',
        'catwetqtemp' : 'bioclim_08',
        'catanngromega' : 'megathermgrowindex',
        'catanngromeso' : 'mesothermgrowindex',
        'catanngromicro' : 'microthermgrowindex',
        'catgromegaseas' : 'megaseasongrowindex',
        'catgromesoseas' : 'mesoseasongrowindex',
        'catgromicroseas' : 'microseasongrowindex',
        'caterosivity' : 'rainerosivityfactor',
        'suberosivity' : 'rainerosivityfactor',
        'strannrad' : 'bioclim_20',
        'stranntemp' : 'bioclim_01',
        'strcoldmthmin' : 'bioclim_06',
        'strhotmthmax' : 'bioclim_05',
        'strcoldqtemp' : 'bioclim_11',
        'strdryqtemp' : 'bioclim_09',
        'strwetqtemp' : 'bioclim_08',
        'strannrain' : 'bioclim_12',
        'strdryqrain' : 'bioclim_17',
        'strwetqrain' : 'bioclim_16',
        'strwarmqrain' : 'bioclim_18',
        'strcoldqrain' : 'bioclim_19',
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
        'cat_solpawhc' : 'waterHoldCapacity',
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
        'nppbaseann' : 'nppann', 
        'nppbase1' : 'nppmon',   
        'nppbase2' : 'nppmon',   
        'nppbase3' : 'nppmon',   
        'nppbase4' : 'nppmon',   
        'nppbase5' : 'nppmon',   
        'nppbase6' : 'nppmon',   
        'nppbase7' : 'nppmon',   
        'nppbase8' : 'nppmon',   
        'nppbase9' : 'nppmon',   
        'nppbase10' : 'nppmon',  
        'nppbase11' : 'nppmon',  
        'nppbase12' : 'nppmon',  
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

    def parse_zip_filename(self, srcfile):
        """
        parse an absolute path to a zip file and return a dict
        with information extracted from full path and filename
        """
        rasterfile = os.path.basename(srcfile)
        boundtype = None
        if rasterfile == self.CATCHMENT_RASTER:
            boundtype = 'catchment'
        elif rasterfile == self.STREAM_RASTER:
         boundtype = 'stream'

        if boundtype is None:
            raise Exception("Invalid boundary file {}".format(srcfile))

        return {
            'boundtype': boundtype
        }

    def parse_attribute(self, attrname, dstype):
        """
        parse the attribute name in a table.
        """
        layerid = self.BCCVL_LAYER_TYPES[attrname]
        if dstype == 'rdi':
            version = '1.1.7'
            gdbfilename = self.RDI_ATTRIBUTE_FILE
        else:
            version = '1.1.5'
            gdbfilename = self.ATTRIBUTE_FILE
        md = { 
            'layerid': layerid, 
            'version': version,
            'dstype': dstype,
            'gdbfilename': gdbfilename
        }
        if layerid == 'nppmon':
            md['month'] = int(attrname.split('nppbase')[1])

        if layerid.startswith('bioclim_'):
            md['year'] = 1958
            md['year_range'] = '1921-1995'
        return md

    def gdal_options(self, md):
        """
        options to add metadata for the tiff file
        """
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE', '-norat']
        options += ['-mo', 'version={}'.format(md['version'])]
        if 'month' in md:
            options += ['-mo', 'month={}'.format(md['month'])]
        if 'year' in md:
            options += ['-mo', 'year_range={}-{}'.format(md['year'], md['year'])]
            options += ['-mo', 'year={}'.format(md['year'])]
        return options

    def target_dir(self, destdir, srcfile):
        md = self.parse_zip_filename(srcfile)
        dirname = 'geofabric_{}'.format(md['boundtype'])
        return os.path.join(destdir, dirname)

    def destfilename(self, destdir, md):
        """
        Generate file name for output tif file.
        """
        # Only nppmon has month
        layername = md['layerid']
        if layername == 'nppmon':
            layername += md['month']

        return (
            os.path.basename(destdir) +
            '_' +
            md['dstype'] +
            '_' +
            layername.replace('_', '-') +
            '.tif'
        )

    def filter_srcfiles(self, srcfile):
        """
        return False to skip this srcfile (zip file)
        """
        return os.path.basename(srcfile) in [self.CATCHMENT_RASTER, self.STREAM_RASTER]


    def get_attribute(self, attrname, tablename, attrgdbfile):
        # TODO: this method is terribly slow
        #       maybe we could use ogr2ogr to convert FileGDB to something that's faster?
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
                value = self.NODATA_VALUE
            values[segmentno] = value
        return (valueType, values)


    def extractAsGeotif(self, template, bandData, attrname, tablename, attrgdbfile):
        # Get the attribute values and type (i.e. 0 = integer)
        # TODO: maybe read whole table (or even file) at once ... read one attribute takes ages ~100MB ram?
        # dtype is OGR Data type
        dtype, values = self.get_attribute(attrname, tablename, attrgdbfile)
        pixel_dtype = GDT_Int32 if dtype == ogr.OFTInteger else GDT_Float32
        # gdal_array.GDALTypeCodeToNumericTypeCode(pixel_dtype) 
        value_dtype = numpy.int32 if dtype == ogr.OFTInteger else numpy.float32

        ofd, outfilename = tempfile.mkstemp(suffix='.tif', prefix=attrname)
        os.close(ofd)
        try:
            # Create dataset for the layer output
            rasterLayer = gdal.Open(template)
            driver = rasterLayer.GetDriver()
            rows = rasterLayer.RasterYSize
            cols = rasterLayer.RasterXSize

            outData = None
            outDataset = driver.Create(outfilename, cols, rows, 1, pixel_dtype, ['COMPRESS=LZW', 'TILED=YES'])
            if outDataset is None:
                raise Exception('Could not create {}'.format(outfilename))

            # TODO: gdal_array.CopyDataSetInfo?
            # georeference the image and set the projection
            outDataset.SetGeoTransform(rasterLayer.GetGeoTransform())
            outDataset.SetProjection(rasterLayer.GetProjection())
            restarLayer = None

            mapfunc = numpy.vectorize(values.get, otypes=[value_dtype])
            outData = numpy.memmap(
                tempfile.NamedTemporaryFile(prefix=attrname),
                dtype=value_dtype, shape=(rows, cols)
            )
            # arbitrary blocksize as we calc from numpy to numpy
            ysize = 256
            for y in range(0, rows, ysize):  # ysize):
                if y + ysize < rows:
                    r = ysize
                else:
                    r = rows - y
                outData[y:y+r,:] = mapfunc(bandData[y:y+r,:], self.NODATA_VALUE)

            # write the data
            outBand = outDataset.GetRasterBand(1)
            # TODO: write  by blocks?
            # write band data by blocks into numpy array
            xsize, ysize = outBand.GetBlockSize()
            for y in range(0, rows, ysize):
                if y + ysize < rows:
                    r = ysize
                else:
                    r = rows - y
                outBand.WriteArray(outData[y:y+r,:], 0, y)

            # flush data to disk, set the NoData value and calculate stats
            outBand.FlushCache()
            outBand.SetNoDataValue(self.NODATA_VALUE)
            
        finally:
            # Release dataset
            if outBand:
                outBand = None
            if outDataset:
                outDataset = None
            if outData is not None:
                outData = None
        return outfilename


    def convert(self, srcfile, destdir):
        """convert .asc.gz files in folder to .tif in dest
        """
        # TODO: looping does not quite fit processing pattern
        #       the full geofabric dataset get's converted into workdir,
        #       before it is being moved to final destination
        #       PROBLEMs:
        #          - workdir may not be able to hold full data
        #          - if anything goes wrong all work has to be redone because loop nothing has been written to final destination (workdir is temp as well)  

        # Open the catchmen/stream boundary raster layer
        rasterLayer = gdal.Open(srcfile)
        if rasterLayer is None:
            raise Exception('Could not open file {}'.format(rasterfile))

        # read in the band data and get info about it
        band1 = rasterLayer.GetRasterBand(1)
        rows = rasterLayer.RasterYSize
        cols = rasterLayer.RasterXSize
        # either write to memmap array or look at gdal virtual datatypes?
        bandData = numpy.memmap(
            tempfile.NamedTemporaryFile(prefix=os.path.basename(srcfile)),
            dtype=gdal_array.GDALTypeCodeToNumericTypeCode(band1.DataType),
            shape=(rows, cols)
        )
        # read band data by blocks into numpy array
        xsize, ysize = band1.GetBlockSize()
        for y in range(0, rows, ysize):
            if y + ysize < rows:
                r = ysize
            else:
                r = rows - y
            bandData[y:y+r,:] = band1.ReadAsArray(0, y, cols, r)
        band1 = None
        rasterLayer = None

        parsed_zip_md = self.parse_zip_filename(srcfile)
        pool = futures.ProcessPoolExecutor(self.max_processes)

        for dstype, tablename in self.GEOFABRIC_TABLES[parsed_zip_md.get('boundtype')]:    
            # For each attribute in the table, create a layer geotif file 
            results = []
            tmpfiles = []
            try:
                for attrname in tqdm(self.GEOFABRIC_ATTRIBUTES[parsed_zip_md.get('boundtype')].get(dstype, {}),
                                    desc="build jobs"):
                    parsed_md = copy.copy(parsed_zip_md)
                    parsed_md.update(
                        self.parse_attribute(attrname, dstype)
                    )
                    # apply scale and offset
                    if parsed_md['layerid'] in self.SCALES:
                        parsed_md['scale'] = self.SCALES[parsed_md['layerid']]
                    if parsed_md['layerid'] in self.OFFSETS:
                        parsed_md['offset'] = self.OFFSETS[parsed_md['layerid']]
                    destfilename = self.destfilename(destdir, parsed_md)

                    # extract attribute data from the associated table
                    attr_gdbfilename = os.path.join(os.path.dirname(srcfile), parsed_md['gdbfilename'])
                    # TODO: can we move this into subprocess as well? (bandData as memmap file? rasterLayer as template file?)
                    #       either override run_gdal or callback in returned process?
                    tmpfile = self.extractAsGeotif(srcfile, bandData, attrname, tablename, attr_gdbfilename)
                    tmpfiles.append(tmpfile)

                    #  Run gdal translate to attach metadata
                    gdaloptions = self.gdal_options(parsed_md)
                    # output file name
                    destpath = os.path.join(destdir, destfilename)
                    # run gdal translate
                    cmd = ['gdal_translate']
                    cmd.extend(gdaloptions)
                    results.append(
                        pool.submit(run_gdal, cmd, tmpfile, destpath, parsed_md)
                    )

                for result in tqdm(futures.as_completed(results),
                                        desc=os.path.basename(srcfile),
                                        total=len(results)):
                    if result.exception():
                        tqdm.write("Job failed")
                        raise result.exception()
            finally:
                # TODO: this keeps all tmpfiles until we have finished converting everything to WORKDIR (not final dir)
                #       this may consume a lot of tmp space
                for f in tmpfiles:
                    os.remove(f)

        # Close the raster layer
        rasterLayer = None


def main():
    converter = GeofabricConverter()
    converter.main()


if __name__ == "__main__":
    main()
