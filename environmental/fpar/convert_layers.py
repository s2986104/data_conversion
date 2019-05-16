#!/usr/bin/env python
import os.path
import copy
import glob
import tempfile

from concurrent import futures
from tqdm import tqdm

from data_conversion.utils import get_vsi_path
from data_conversion.converter import BaseConverter, run_gdal

from fpar_stats import get_file_lists, raster_chunking_stats, calc_cov, write_array_to_raster, stat_filename

class FparConverter(BaseConverter):

    # get layer id from filename within zip file
    def parse_filename(self, filename):
        parts = os.path.basename(filename).split('.')
        return {
            'layerid': 'fpar',
            'year': int(parts[1]),
            'month': int(parts[2]),
            'year_range': '{}-{}'.format(parts[1], parts[1])
        }

    def target_dir(self, destdir, srcfile):
        root = os.path.join(destdir, 'fpar-250m')
        return root

    def destfilename(self, destdir, md):
        """
        generate file name for output tif file.
        """
        filename = '{}_{}'.format(
                        os.path.basename(destdir),
                        md['layerid'].replace('_', '-')
                    )
        if 'month' in md:
            filename += '_{:02d}'.format(md['month'])
        if 'year' in md:
            filename += '_{:04d}'.format(md['year'])
        filename += '.tif'
        return filename

    def gdal_options(self, md):
        # options to add metadata for the tiff file
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE']
        options += ['-mo', 'year_range={}'.format(md['year_range'])]
        options += ['-mo', 'year={}'.format(md['year'])]
        if md.get('month'):
            options += ['-mo', 'month={}'.format(md['month'])]
        return options

    def skip_zipinfo(self, zipinfo):
        # expect zipinfo as a tif file
        # default ignore directories
        if os.path.isdir(zipinfo):
            return True
        # ignore none .tif
        _, ext = os.path.splitext(zipinfo)
        if ext not in ('.tif'):
            return True
        return False

    def get_sourcefiles(self, srcfile):
        # return only the fpar folder
        if os.path.isdir(srcfile):
            return [os.path.join(srcfile, 'fpar')]
        else:
            raise Exception("source must be a directory")

    def check_results(self, results, desc=None):
       for result in tqdm(futures.as_completed(results),
                          desc=desc,
                          total=len(results)):
           if result.exception():
               tqdm.write("Job failed")
               raise result.exception()
 

    def convert_stats(self, stats, template, destroot, md):
        """Load in raster files in chunks to reduce memory demands,
        calculate statistics, and save to file.

        Keyword arguments:
        stats -- dictionary with numpy arrayrs to store as datasets
        template -- a gdal dataset to be used as template to create new ones
        destroot -- the folder to store the final tif file in
        md -- metadata
        Returns: None.
        """

        # Write the results to raster format with appropriate filenames
        pool = futures.ProcessPoolExecutor(self.max_processes)
        results = []
        tmpfiles =[]
        try:
            for stattype, statarr in tqdm(stats.items(), desc='build stats'):
                layerid = 'fpar{}'.format(stattype)
                md['layerid'] = layerid
                # apply scale and offset
                if md['layerid'] in self.SCALES:
                    md['scale'] = self.SCALES[md['layerid']]
                if md['layerid'] in self.OFFSETS:
                    md['offset'] = self.OFFSETS[md['layerid']]

                _, tmpfile = tempfile.mkstemp(prefix=stattype + '_', suffix='.tif')
                write_array_to_raster(tmpfile, statarr, template)
                tmpfiles.append(tmpfile)

                #run gdal command to attach metadata
                gdaloptions = self.gdal_options(md)
                # output stats file name
                outfile = stat_filename(destroot, md)
                # run gdal translate
                cmd = ['gdal_translate']
                cmd.extend(gdaloptions)
                results.append(
                    pool.submit(run_gdal, cmd, tmpfile, outfile, md)
                )

                self.check_results(results, desc=os.path.basename(outfile))
        finally:
            for tmpf in tmpfiles:
                os.remove(tmpf)

    def fpar_stats(self, destroot, srctif_dir='tifs'):
        # Generate the lists for global, long-term monthly, and yearly raster stacks
        (glbl, mntly, growyrly, calyrly) = get_file_lists(srctif_dir)

        # Calculate the statistics
        #print("Calculating monthly stats ...")
        pbar = tqdm(total=4, desc="Calculate stats")
        for mth in tqdm(mntly.keys(), desc='monthly'):
            md = {
                'fnameformat': 'monthly',
                'month': int(mth),
                'year': 2007,
                'year_range': '2000-2014'
            }
            stats = raster_chunking_stats(mntly[mth])
            self.convert_stats(stats, mntly[mth][0], destroot, md)
            stats = None
        pbar.update()

        #print("Calculating grow-yearly stats ...")
        for yr in tqdm(growyrly.keys(), desc='grow-yearly'):
            year = int(yr)
            md = {
                'fnameformat': 'growyearly',
                'year': year,
                'year_range': "{:04d}-{:04d}".format(year, year + 1)
            }
            stats = raster_chunking_stats(growyrly[yr])
            self.convert_stats(stats, growyrly[yr][0], destroot, md)
            stats = None
        pbar.update()

        #print("Calculating calendar yearly stats ...")
        for yr in tqdm(calyrly.keys(), desc='calenda-ryearly'):
            year = int(yr)
            md = {
                'fnameformat': 'calyearly',
                'year': year,
                'year_range': "{:04d}-{:04d}".format(year, year)
            }       
            stats = raster_chunking_stats(calyrly[yr])
            self.convert_stats(stats, calyrly[yr][0], destroot, md)
            stats = None
        pbar.update()

        #print("Calculating global stats ...")
        md = {
            'fnameformat': 'global',
            'year': 2007,
            'year_range': '2000-2014'
        }        
        stats = raster_chunking_stats(glbl)
        stats['cov'] = calc_cov(glbl)
        self.convert_stats(stats, glbl[0], destroot, md)
        pbar.update()
        pbar.close()

    def convert(self, srcfile, destdir):
        """conver the tif layer and then compute the stats.
        """
        parsed_zip_md = self.parse_zip_filename(srcfile)
        pool = futures.ProcessPoolExecutor(3)
        results = []
        srcfiles = sorted(glob.glob(os.path.join(srcfile, '**', '*.tif'), recursive=True))
        for filename in tqdm(srcfiles, desc="build jobs"):
            if self.skip_zipinfo(filename):
                continue

            parsed_md = copy.deepcopy(parsed_zip_md)
            parsed_md.update(
                self.parse_filename(filename)
            )
            # apply scale and offset
            if parsed_md['layerid'] in self.SCALES:
                parsed_md['scale'] = self.SCALES[parsed_md['layerid']]
            if parsed_md['layerid'] in self.OFFSETS:
                parsed_md['offset'] = self.OFFSETS[parsed_md['layerid']]
            destfilename = self.destfilename(destdir, parsed_md)
            srcurl = get_vsi_path(filename)
            gdaloptions = self.gdal_options(parsed_md)
            # output file name
            destpath = os.path.join(destdir, destfilename)
            # run gdal translate
            cmd = ['gdal_translate']
            cmd.extend(gdaloptions)
            results.append(
                pool.submit(run_gdal, cmd, srcurl, destpath, parsed_md)
            )
        self.check_results(results, os.path.basename(srcfile))

        # Calculate the fpar statistics for the tiff files
        self.fpar_stats(destdir, srcfile)


def main():
    converter = FparConverter()
    converter.main()


if __name__ == "__main__":
    main()
