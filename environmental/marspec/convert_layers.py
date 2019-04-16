#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseConverter


class MarspecConverter(BaseConverter):

    def parse_zip_filename(self, srcfile):     

        # layerid, year
        return {
            'layerid': 'Bathymetry',
            'year': '2002',
            'year_range': '1955-2010'
        }

    def target_dir(self, destdir, srcfile):
        fmd = self.parse_zip_filename(srcfile)
        year = fmd['year']
        dirname = 'marspec_{0}'.format(year)
        root = os.path.join(destdir, dirname)
        return root

    def gdal_options(self, md):
        # options to add metadata for the tiff file
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE']
        options += ['-mo', 'year_range={}'.format(md['year_range'])]
        options += ['-mo', 'year={}'.format(md['year'])]
        return options


def main():
    converter = MarspecConverter()
    converter.main()


if __name__ == "__main__":
    main()
