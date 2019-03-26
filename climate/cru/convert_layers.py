#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseConverter


class CRUConverter(BaseConverter):

    def parse_filename(self, filename):
        fname = os.path.splitext(os.path.basename(filename))[0]
        _, layerid, year = fname.split('_')
        return {
            'layerid': 'bioclim_{:02d}'.format(int(layerid)),
            'year': year
        }

    def gdal_options(self, md):
        year = int(md['year'])
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE', '-norat']
        options += ['-mo', 'year_range={}-{}'.format(year-14, year+15)]
        options += ['-mo', 'year={}'.format(year)]
        return options

    def destfilename(self, destdir, md):
        return 'cruclim_{}_{}.tif'.format(
            md['layerid'], md['year']
        ) 

    def target_dir(self, destdir, srcfile):
        dirname = 'cruclim'
        root = os.path.join(destdir, dirname)
        return root


def main():
    converter = CRUConverter()
    converter.main()


if __name__ == "__main__":
    main()
