#!/usr/bin/env python
import os
import os.path
import tempfile
import shutil
import copy
import zipfile
import subprocess
from concurrent import futures
from osgeo import gdal, ogr, osr
import os

from data_conversion.converter import BaseConverter, run_gdal

LAYERINFO = {
    'Multi-resolution_Valley_Bottom_Flatness__MrVBF__3__resolution_.zip': {
        'layerid': 'mrvbf', 
        'version': '1.0',
        'fragpath': 'MrVBF_3s/mrvbf6g-a5_3s_median/tiles/*/*/e1*',
        'ratdata': (    # value, threshold, resolution, interpretation
            (0, '', 30, 'Erosional'),
            (1, '16', 30, 'Small hillside deposit'),
            (2, '8', 30, 'Narrow valley floor'),
            (3, '4', 90, ''),
            (4, '2', 270, 'Valley floor'),
            (5, '1', 800, 'Extensive valley floor'),
            (6, '0.5', 2400, ''),
            (7, '0.25', 7200, 'Depositional basin'),
            (8, '0.125', 22000, ''),
            (9, '0.0625', 66000, 'Extensive depositional basin')
        )
    },
    'Multi-resolution_Ridge_Top_Flatness__MrRTF__3__resolution_.zip': {
        'layerid': 'mrrtf', 
        'version': '1.0',
        'fragpath': 'MrRTF_3s/mrrtf6g-a5_3s_median/tiles/*/*/e1*',
        'ratdata': (    # value, threshold, resolution
            (0, '', 30),
            (1, '16', 30),
            (2, '8', 30),
            (3, '4', 90),
            (4, '2', 270),
            (5, '1', 800),
            (6, '0.5', 2400),
            (7, '0.25', 7200),
            (8, '0.125', 22000),
            (9, '0.0625', 66000)
        )
    }
}

class AusTopographyConverter(BaseConverter):

    def parse_zip_filename(self, srcfile):
        basename = os.path.basename(srcfile)
        # layerid, year, year_range
        return {
            'layerid': LAYERINFO[basename].get('layerid'),
            'version': LAYERINFO[basename].get('version')
        }

    def target_dir(self, destdir, srcfile):
        md = self.parse_zip_filename(srcfile)
        root = os.path.join(destdir, 'aus-topography-90m-{}'.format(md['layerid']))
        return root

    def gdal_options(self, md):
        # options to add metadata for the tiff file
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE']
        options += ['-co', 'ZLEVEL=9']
        options += ['-mo', 'version={}'.format(md['version'])]
        return options

    def run_buildvrt(self, srcdir, infile):
        _, ofile = tempfile.mkstemp(suffix='.vrt')

        # Need to run it in shell to prevent failure.
        cmd = 'gdalbuildvrt {} {}'.format(ofile, infile)
        ret = subprocess.run(
                cmd,
                shell=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        ret.check_returncode()
        return ofile


    def create_rat(self, infile, ratdata):
        hasInterpretation = len(ratdata[0]) == 4

        ds = gdal.Open(infile, gdal.GA_Update)
        sr = osr.SpatialReference()
        # Horizontal WGS84 + Vertical EPG96
        sr.SetFromUserInput('EPSG:4326+5773')
        ds.SetProjection(sr.ExportToWkt())
        rb = ds.GetRasterBand(1)
        rb.SetColorInterpretation(gdal.GCI_Undefined)
        stats = rb.ComputeStatistics(False)
        rb.SetStatistics(*stats)
        rat = gdal.RasterAttributeTable()
        rat.CreateColumn('VALUE', gdal.GFT_Integer, gdal.GFU_MinMax)
        rat.CreateColumn('Threshold Slope (%)', gdal.GFT_String, gdal.GFU_Generic)
        rat.CreateColumn('Resolution (approx meter)', gdal.GFT_Integer, gdal.GFU_Generic)
        if hasInterpretation:
            rat.CreateColumn('Interpretation', gdal.GFT_String, gdal.GFU_Name)
        for data in ratdata:
            rat.SetValueAsInt(data[0], 0, data[0])    # value
            rat.SetValueAsString(data[0], 1, data[1]) # threshold 
            rat.SetValueAsInt(data[0], 2, data[2])    #resolution
            if hasInterpretation:
                rat.SetValueAsString(data[0], 3, data[3])  # interpretation
        rb.SetDefaultRAT(rat)

    def convert(self, srcfile, destdir):
        """convert .zip files in folder to .tif in dest
        """
        zipfilename = os.path.basename(srcfile)
        parsed_zip_md = self.parse_zip_filename(srcfile)
        pool = futures.ProcessPoolExecutor(3)
        results = []
        vrtfile = None
        try:
            with tempfile.TemporaryDirectory() as tempdirname:
                # Extract files
                with zipfile.ZipFile(srcfile) as srczip:
                    srczip.extractall(tempdirname)

                # Make a vrt file
                infile = os.path.join(tempdirname, LAYERINFO[zipfilename].get('fragpath'))
                vrtfile = self.run_buildvrt(tempdirname, infile)

                parsed_md = copy.copy(parsed_zip_md)
                # apply scale and offset
                if parsed_md['layerid'] in self.SCALES:
                    parsed_md['scale'] = self.SCALES[parsed_md['layerid']]
                if parsed_md['layerid'] in self.OFFSETS:
                    parsed_md['offset'] = self.OFFSETS[parsed_md['layerid']]
                destfilename = self.destfilename(destdir, parsed_md)
                gdaloptions = self.gdal_options(parsed_md)
                # output file name
                destpath = os.path.join(destdir, destfilename)
                # run gdal translate
                cmd = ['gdal_translate']
                cmd.extend(gdaloptions)
                results.append(
                    pool.submit(run_gdal, cmd, vrtfile, destpath, parsed_md)
                )
                for result in futures.as_completed(results):
                    if result.exception():
                        raise result.exception()

            # create RAT
            ratdata = LAYERINFO[zipfilename].get('ratdata')
            self.create_rat(destpath, ratdata)

        except Exception as e:
            print('Error:', e)
            raise e
        finally:
            if vrtfile:
                os.remove(vrtfile)

def main():
    converter = AusTopographyConverter()
    converter.main()


if __name__ == "__main__":
    main()
