#!/usr/bin/env python
import os.path
from datetime import datetime

from data_conversion.converter import BaseConverter


class AwapConverter(BaseConverter):

    def parse_zip_filename(self, srcfile):
        """
        Parse filename of the format 19870101_19871231.FWDis.run26j.flt.zip 
        to get layerid
        """
        # this should always parse source file ... we know the dest file anyway
        basename = os.path.basename(srcfile)
        parts = os.path.splitext(basename)[0].split('.')

        date = parts[0].split('_')[0]
        year = datetime.strptime(date, '%Y%m%d').year
        return {
            'year': year,
            'run': parts[2]
        }

    def parse_filename(self, filename):
        # AWAP/Run26h/FWDis/pcr_ann_FWDis_20051231.flt
        parts = os.path.splitext(os.path.basename(filename))[0].split('_')
        date = parts[-1]
        year = datetime.strptime(date, '%Y%m%d').year
        layerid = '_'.join(parts[:-1])

        return {
            'layerid': layerid,
            'year': year
        }

    def filter_srcfiles(self, srcfile):
        """
        return False to skip this srcfile (zip file)
        """
        # only process zip files for Run26j
        return 'run26j' in os.path.basename(srcfile).split('.')

    def skip_zipinfo(self, zipinfo):
        """
        return true to ignore this zipinfo entry
        """
        # default ignore directories
        if zipinfo.is_dir():
            return True
        # ignore none .tif, .asc files
        _, ext = os.path.splitext(zipinfo.filename)
        if ext not in ('.flt',):
            return True

        # only want annual layer
        md = self.parse_filename(zipinfo.filename)
        return 'ann' not in md['layerid'].split('_')

    def gdal_options(self, md):
        """
        options to add metadata for the tiff file
        """
        year = md['year']
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE', '-norat']
        options += ['-mo', 'year_range={}-{}'.format(year, year)]
        options += ['-mo', 'year={}'.format(year)]
        return options

    def target_dir(self, destdir, srcfile):
        md = self.parse_zip_filename(srcfile)
        dirname = 'awap_{}_{}'.format(md['run'], md['year'])
        return os.path.join(destdir, dirname)


def main():
    converter = AwapConverter()
    converter.main()


if __name__ == "__main__":
    main()
