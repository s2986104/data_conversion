#!/usr/bin/env python
import os.path
import numpy as np
import tempfile
import shutil
import copy
import zipfile
from concurrent import futures
from osgeo import gdal, ogr
from tqdm import tqdm


from data_conversion.utils import get_vsi_path
from data_conversion.converter import BaseConverter, run_gdal

REDUCED_RAT   = 'bccvl_national-dynamic-land-cover-rat-reduced.tif.aux.xml'

LAYERINFO = {
    'scene01-dlcdv1_class': ('dlcdv1_class', 2004),
    'scene01-trend_evi_min': ('trend_evi_min', 2004),
    'scene01-trend_evi_max': ('trend_evi_max', 2004),
    'scene01-trend_evi_mean': ('trend_evi_mean', 2004)
}

# this column map has been handcrafted,
# info can be read from source file with ogrinfo -al DLCDv1_Class.tif.vat.dbf
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


class NDLCConverter(BaseConverter):

    def filter_srcfiles(self, srcfile):
        return os.path.basename(srcfile) != 'Reference_documents.zip'

    def parse_zip_filename(self, srcfile):
        basename = os.path.basename(srcfile)
        fname, _ = os.path.splitext(basename)
        # layerid, year
        return {
            'layerid': LAYERINFO[fname.lower()][0],
            'year': LAYERINFO[fname.lower()][1],
        }

    # get layer id from filename within zip file
    def parse_filename(self, filename):
        basename = os.path.basename(filename).lower()
        layerid, _ = os.path.splitext(basename)
        return {
            'layerid': layerid,
        }

    def target_dir(self, destdir, srcfile):
        root = os.path.join(destdir, 'ndlc-2004-250m')
        return root

    def gdal_options(self, md):
        # options to add metadata for the tiff file
        year = md['year']
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE']
        options += ['-mo', 'year_range={}-{}'.format(year-4, year+4)]
        options += ['-mo', 'year={}'.format(year)]
        if not md['layerid'] in ('dlcdv1_class', 'dlcdv1_class_reduced'):
            options += ['-norat', '-stats']  # force compute stats so that other stats data is discarded
        return options

    def reclassify(self, tiffname, class_map, destfile):
        driver=gdal.GetDriverByName('GTiff')
        tiffile = gdal.Open(tiffname)
        band = tiffile.GetRasterBand(1)
        data = band.ReadAsArray()

        # reclassification
        for newval, rangeval in class_map.items():
            data[(data>=rangeval[0]) & (data<=rangeval[1])] = newval

        # create new file
        file2 = driver.Create(destfile, tiffile.RasterXSize , tiffile.RasterYSize , 1)
        file2.GetRasterBand(1).WriteArray(data)

        # spatial ref system
        proj = tiffile.GetProjection()
        georef = tiffile.GetGeoTransform()
        file2.SetProjection(proj)
        file2.SetGeoTransform(georef)
        file2.FlushCache()

    def get_rat_from_vat(self, filename):
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

    def convert(self, srcfile, destdir):
        """convert .asc.gz files in folder to .tif in dest
        """
        parsed_zip_md = self.parse_zip_filename(srcfile)
        pool = futures.ProcessPoolExecutor(3)
        results = []
        tfname1 = None
        tfname2 = None
        with zipfile.ZipFile(srcfile) as srczip:
            for zipinfo in tqdm(srczip.filelist, desc="build jobs"):
                if self.skip_zipinfo(zipinfo):
                    continue

                parsed_md = copy.copy(parsed_zip_md)
                parsed_md.update(
                    self.parse_filename(zipinfo.filename)
                )
                # apply scale and offset
                if parsed_md['layerid'] in self.SCALES:
                    parsed_md['scale'] = self.SCALES[parsed_md['layerid']]
                if parsed_md['layerid'] in self.OFFSETS:
                    parsed_md['offset'] = self.OFFSETS[parsed_md['layerid']]
                destfilename = self.destfilename(destdir, parsed_md)
                srcurl = get_vsi_path(srcfile, zipinfo.filename)
                gdaloptions = self.gdal_options(parsed_md)
                # output file name
                destpath = os.path.join(destdir, destfilename)
                # run gdal translate
                cmd = ['gdal_translate']
                cmd.extend(gdaloptions)
                if zipinfo.filename.lower().find('dlcdv1_class.tif') < 0:
                    results.append(
                        pool.submit(run_gdal, cmd, srcurl, destpath, parsed_md)
                    )
                else:
                    # Special handling for DLCDV1_Class.
                    # 1. Need to attach the associated RAT table
                    # 2. Add a reduced classification data layer for DLCDv1_Class
 
                    # Load the rat table from vat.dbf file
                    ratfile = '{}.vat.dbf'.format(srcurl)
                    rat = self.get_rat_from_vat(ratfile)

                    # open dataset
                    ds = gdal.Open(srcurl)
                    # create a temp tif file and attach the RAT table
                    driver = ds.GetDriver()
                    _, tfname1 = tempfile.mkstemp(suffix='.tif')
                    newds = driver.CreateCopy(tfname1,
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

                    # run gdal translate to attach associated metadata
                    results.append(
                        pool.submit(run_gdal, cmd, tfname1, destpath, parsed_md)
                    )


                    # Add reduced classification data layer for DLCDv1_Class
                    class_map = {1: (1,10), 2: (11,23), 3: (24,30), 4: (31,32), 5: (33,34)}
                    _, tfname2 = tempfile.mkstemp(suffix='.tif')
                    self.reclassify(srcurl, class_map, tfname2)

                    reduced_md = {'layerid': 'dlcdv1_class_reduced', 'year': 2004}
                    gdaloptions = self.gdal_options(reduced_md)
                    reduced_destfilename = os.path.splitext(os.path.basename(destfilename))[0] + '-reduced.tif'
                    destpath = os.path.join(destdir, reduced_destfilename)

                    # copy the RAT file for the reduced DLCDv1_Class
                    shutil.copy(REDUCED_RAT, os.path.join(destdir, reduced_destfilename + '.aux.xml'))

                    # run gdal translate on the reduced temp file
                    cmd = ['gdal_translate']
                    cmd.extend(gdaloptions)
                    results.append(
                        pool.submit(run_gdal, cmd, tfname2, destpath, reduced_md)
                    )


        for result in tqdm(futures.as_completed(results),
                                desc=os.path.basename(srcfile),
                                total=len(results)):
            if result.exception():
                tqdm.write("Job failed")
                for fp in (tfname1, tfname2):
                    if fp:
                        os.remove(fp)
                raise result.exception()
        # Renove temp file if any
        for fp in (tfname1, tfname2):
            if fp:
                os.remove(fp)


def main():
    converter = NDLCConverter()
    converter.main()


if __name__ == "__main__":
    main()
