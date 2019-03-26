#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseConverter


LAYERINFO = {
    'precSUM': 'mprec',
    'evap': 'mevap',
    'vapp': 'mvapp',
    'tmax': 'mtmax',
    'tmin': 'mtmin'
}

class ANUClimConverter(BaseConverter):

    def parse_filename(self, filename):
        fname = os.path.splitext(os.path.basename(filename))[0]
        layerid, month = fname.split('_')
        # in case of .asc.gz
        month = month.split('.')[0]
        layerid = LAYERINFO[layerid]
        return {
            'layerid': layerid,
            'month': int(month),
            'year': 1990,
        }

    def gdal_options(self, md):
        year = int(md['year'])
        month = int(md['month'])
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE', '-norat']
        options += ['-mo', 'year_range={}-{}'.format(year-14, year+15)]
        options += ['-mo', 'year={}'.format(year)]
        options += ['-mo', 'month={}'.format(month)]
        return options

    def skip_zipinfo(self, zipinfo):
        if zipinfo.is_dir():
            # skip dir entries
            return True
        if not zipinfo.filename.endswith('.tif') and \
           not zipinfo.filename.endswith('.asc.gz'):
            return True
        return False

    def destfilename(self, destdir, md):
        return 'anuclim_{}_{}.tif'.format(
            md['layerid'], md['month']
        )

    def target_dir(self, destdir, srcfile):
        dirname = 'anuClim'
        root = os.path.join(destdir, dirname)
        return root

    def filter_srcfiles(self, srcfile):
        return 'daily' not in srcfile


def main():
    converter = ANUClimConverter()
    converter.main()


if __name__ == "__main__":
    main()
