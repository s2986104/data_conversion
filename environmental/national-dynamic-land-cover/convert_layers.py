#!/usr/bin/env python
import os.path
import numpy as np
import tempfile
import shutil
import copy
import zipfile
from concurrent import futures
from osgeo import gdal
from tqdm import tqdm



from data_conversion.converter import BaseConverter

REDUCED_RAT   = 'bccvl_national-dynamic-land-cover-rat-reduced.tif.aux.xml'

LAYERINFO = {
    'Scene01-DLCDv1_Class.zip': ('DLCDv1_Class', 2004),
    'Scene01-trend_evi_min.zip': ('trend_evi_min', 2004),
    'Scene01-trend_evi_max.zip': ('trend_evi_max', 2004),
    'Scene01-trend_evi_mean.zip': ('trend_evi_mean', 2004)
}


class NDLCConverter(BaseConverter):

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
        layerid = os.path.basename(filename)
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
        options += ['-co', 'COMPRESS=DEFLATE', '--config']
        options += ['-mo', 'year_range={}-{}'.format(year-4, year+4)]
        options += ['-mo', 'year={}'.format(year)]
        return options

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

        # spatial ref system
        proj = tiffile.GetProjection()
        georef = tiffile.GetGeoTransform()
        file2.SetProjection(proj)
        file2.SetGeoTransform(georef)
        file2.FlushCache()

     def convert(self, srcfile, destdir):
        """convert .asc.gz files in folder to .tif in dest
        """
        parsed_zip_md = self.parse_zip_filename(srcfile)
        pool = futures.ProcessPoolExecutor(3)
        results = []
        tfname = None
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
                results.append(
                    pool.submit(run_gdal, cmd, srcurl, destpath, parsed_md)
                )

                # add reduced classification data layer for DLCDv1_Class
                if zipinfo.filename.find('DLCDv1_Class.tif') >= 0:
                    class_map = {1: range(1,11), 2: range(11,24), 3: range(24,31), 4: range(31,33), 5: range(33,35)}
                    _, tfname = tempfile.mkstemp(suffix='.tif')
                    reclassify(srcurl, class_map, tfname)

                    reduced_md = {'layerid': 'DLCDv1_Class_Reduced', 'year': 2004}
                    gdaloptions = self.gdal_options(reduced_md)
                    reduced_destfilename = os.path.splitext(os.psth.basename(destfilename))[0] + '-Reduced.tif'
                    destpath = os.path.join(destdir, reduced_destfilename)

                    # copy the RAT file for the reduced DLCDv1_Class
                    shutil.copy(REDUCED_RAT, os.path.join(destdir, reduced_destfilename + '.aux.xml'))

                    # run gdal translate on the reduced temp file
                    cmd = ['gdal_translate']
                    cmd.extend(gdaloptions)
                    results.append(
                        pool.submit(run_gdal, cmd, tfname, destpath, reduced_md)
                    )

        for result in tqdm(futures.as_completed(results),
                                desc=os.path.basename(srcfile),
                                total=len(results)):
            if result.exception():
                tqdm.write("Job failed")
                if tfname:
                    os.remove(tfname)
                raise result.exception()
        # Renove temp file if any
        if tfname:
            os.remove(tfname)

def main():
    converter = NDLCConverter()
    converter.main()


if __name__ == "__main__":
    main()
