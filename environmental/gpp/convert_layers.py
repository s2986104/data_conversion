#!/usr/bin/env python
import os.path
import tempfile
import shutil
from concurrent import futures
import copy
import zipfile
import numpy as np
from osgeo import gdal
from scipy import stats
from tqdm import tqdm


from data_conversion.converter import BaseConverter, run_gdal
from data_conversion.utils import get_vsi_path

MASK_FILE = '/vsizip/{srcdir}/gpp_maxmin_2000_2007.zip/gpp_mask.rst'


LAYER_INFO = {
    # gpp_maxmin_2000_2007
    'gppyrmax_2000_07_molco2yr.rst':    ('gppmax', '2003', (2000, 2007)),
    'gppyrmean_2000_07_molco2yr.rst':   ('gppmean', '2003', (2000, 2007)),
    'gppyrmin_2000_07_molco2yr.rst':    ('gppmin', '2003', (2000, 2007)),
    'gpp_maxmin_2000_2007_gppcov.rst':  ('gppcov', '2003', (2000, 2007)),

    # gpp_year_means2000_2007
    'gppyr_2000_01_molco2m2yr_m.rst':   ('gppmean', '2000', None),
    'gppyr_2001_02_molco2m2yr_m.rst':   ('gppmean', '2001', None),
    'gppyr_2002_03_molco2m2yr_m.rst':   ('gppmean', '2002', None),
    'gppyr_2003_04_molco2m2yr_m.rst':   ('gppmean', '2003', None),
    'gppyr_2004_05_molco2m2yr_m.rst':   ('gppmean', '2004', None),
    'gppyr_2005_06_molco2m2yr_m.rst':   ('gppmean', '2005', None),
    'gppyr_2006_07_molco2m2yr_m.rst':   ('gppmean', '2006', None),
}

class GPPConverter(BaseConverter):

    # get layer id from filename within zip file
    def parse_filename(self, filename):
        fname = os.path.basename(filename).lower()
        layerid, year, year_range = LAYER_INFO[fname]
        return {
            'layerid': layerid,
            'year': year,
            'year_range': year_range
        }

    def target_dir(self, destdir, srcfile):
        fname, _ = os.path.splitext(os.path.basename(srcfile))
        root = os.path.join(destdir, 'gpp-1km', fname.lower())
        return root

    def destfilename(self, destdir, md):
        """
        generate file name for output tif file.
        """
        return (
            os.path.basename(destdir).lower() +
            '_' +
            md['year'] +
            '_' +
            md['layerid'].replace('_', '-') +
            '.tif'
        )

    def gdal_options(self, md):
        # options to add metadata for the tiff file
        year = md['year']
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE']
        yearmin = yearmax = year
        if md['year_range']:
            yearmin, yearmax = md['year_range']
        options += ['-mo', 'year_range={}-{}'.format(yearmin, yearmax)]
        options += ['-mo', 'year={}'.format(year)]
        return options

    def skip_zipinfo(self, zipinfo):
        if zipinfo.is_dir():
            return True
        if not zipinfo.filename.lower().endswith('.rst'):
            # skip non data dirs
            return True
        # also skip all hdr.adf inside a folder ending in _src
        if not os.path.basename(zipinfo.filename).lower() in LAYER_INFO:
            return True
        return False

    def filter_srcfiles(self, srcfile):
        return os.path.basename(srcfile) in ('gpp_maxmin_2000_2007.zip', 'GPP_year_means2000_2007.zip') 

    def mask_file(self, filename, maskfile):
        """Apply the mask and then convert .rst files to .tif
        """
        destpath = None
        srcfile = os.path.splitext(os.path.basename(filename))[0].lower()
        destfile = '{}.tif'.format(srcfile)
        print("Masking {}".format(filename))
        destpath = os.path.join(tempfile.mkdtemp(), destfile)
        ret = os.system(
            'gdal_calc.py -A {0} -B {1} --outfile={2} --calc="A*B" --NoDataValue=-9999'.format(maskfile, filename, destpath)
        )
        if ret != 0:
            raise Exception(
                "can't gdal_cal.py {0} ({1})".format(filename, ret)
            )
        return destpath


    def calc_cov(self, dsfiles):
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

    def write_array_to_raster(self, outfile, dataset, template):
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

        # assume value 0 is due to nodatavalue, set it to -9999.
        dataset[dataset==0] = -9999

        # Set the nodatavalue
        outdata.GetRasterBand(1).SetNoDataValue(-9999)

        # write data to file
        outdata.GetRasterBand(1).WriteArray(dataset)

        # calculate statistics
        outdata.GetRasterBand(1).ComputeStatistics(False)

        # flush data to disk
        outdata.FlushCache()

    def convert(self, srcfile, destdir):
        """convert .asc.gz files in folder to .tif in dest
        """
        parsed_zip_md = self.parse_zip_filename(srcfile)
        pool = futures.ProcessPoolExecutor(3)
        results = []
        yrfiles = []
        tempfiles = []
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

                # Apply the mask 1st.
                mfile = MASK_FILE.format(srcdir=os.path.dirname(srcfile))
                mfile = os.path.join(os.path.dirname(srcurl), mfile)
                masked_tempfile = self.mask_file(srcurl, mfile)
                tempfiles.append(masked_tempfile)

                # output file name
                destpath = os.path.join(destdir, destfilename)
                if os.path.basename(srcfile) == 'GPP_year_means2000_2007.zip':
                    yrfiles.append(destpath)

                # run gdal translate to attach metadata
                cmd = ['gdal_translate']
                cmd.extend(gdaloptions)
                results.append(
                    pool.submit(run_gdal, cmd, masked_tempfile, destpath, parsed_md)
                )

        for result in tqdm(futures.as_completed(results),
                                desc=os.path.basename(srcfile),
                                total=len(results)):
            if result.exception():
                tqdm.write("Job failed")
                for f in tempfiles:
                    shutil.rmtree(os.path.dirname(f))
                raise result.exception()

        # Remove the temp directories
        for f in tempfiles:
            shutil.rmtree(os.path.dirname(f))

        # Lastly, compute the co-variance from the yearly layers
        results = []
        tempfiles = []
        if yrfiles:
            cov = self.calc_cov(yrfiles)
            parsed_md = self.parse_filename('gpp_maxmin_2000_2007_gppcov.rst')     
            gdaloptions = self.gdal_options(parsed_md)       
            covfilename = 'gpp_maxmin_2000_2007_gppcov.tif'

            # write to a temp file
            covtempfile = os.path.join(tempfile.mkdtemp(), covfilename)
            self.write_array_to_raster(covtempfile, cov, yrfiles[0])
            tempfiles.append(covtempfile)

            # output filename
            destpath = os.path.join(destdir, covfilename)

            # run gdal translate to attach metadata
            cmd = ['gdal_translate']
            cmd.extend(gdaloptions)
            results.append(
                pool.submit(run_gdal, cmd, covtempfile, destpath, parsed_md)
            )

            for result in tqdm(futures.as_completed(results),
                                desc=os.path.basename(srcfile),
                                total=len(results)):
                if result.exception():
                    tqdm.write("Job failed")
                    for f in tempfiles:
                        shutil.rmtree(os.path.dirname(f))
                    raise result.exception()

            # Remove the temp directories
            for f in tempfiles:
                shutil.rmtree(os.path.dirname(f))


def main():
    converter = GPPConverter()
    converter.main()


if __name__ == "__main__":
    main()
