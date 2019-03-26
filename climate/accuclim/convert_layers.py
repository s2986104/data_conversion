#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseConverter


class AccuClimConverter(BaseConverter):

    def parse_filename(self, filename):
        fname = os.path.splitext(os.path.basename(filename))[0]
        _, layerid, year = fname.split('_')
        return {
            'layerid': 'bioclim_{:02d}'.format(int(layerid)),
            'year': year
        }

    def gdal_options(self, md):
        """
        options to add metadata for the tiff file
        """
        year = int(md['year'])
        
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE', '-norat']
        options += ['-mo', 'year_range={}-{}'.format(year-14, year+15)]
        options += ['-mo', 'year={}'.format(year)]
        return options

    def destfilename(self, destdir, md):
        return 'accuclim_{}_{}.tif'.format(
            md['layerid'], md['year']
        )

    def target_dir(self, destdir, srcfile):
        dirname = 'accuClim'
        root = os.path.join(destdir, dirname)
        return root


def main():
    converter = AccuClimConverter()
    converter.main()


if __name__ == "__main__":
    main()
