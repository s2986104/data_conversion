#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseConverter


# Layer period
LAYERINFO = {
    'present': {
        'year': 2007,
        'period': '2000-2014'
    },
    '2050': {
        'year': 2045,
        'period': '2040-2050'
    },
    '2100': {
        'year': 2095,
        'period': '2090-2100'
    }
}

class MarineConverter(BaseConverter):

    def parse_zip_filename(self, srcfile):
        # Get layer id from zip filename
        basename = os.path.basename(srcfile).lower()
        fname = basename[:-len('.tif.zip')]
        parts = fname.split('.')
        period = parts[0]
        rcp = None

        if period == 'present':
            parts = parts[1:]
            rcp = 'current'
        else:
            rcp = parts[1]
            parts = parts[2:]

        layerid = '.'.join([i.capitalize() for i in parts])

        # layerid, year
        return {
            'layerid': layerid,
            'year': LAYERINFO[period]['year'],
            'year_range': LAYERINFO[period]['period'],
            'emsc': rcp,
        }

    def target_dir(self, destdir, srcfile):
        fmd = self.parse_zip_filename(srcfile)
        emsc = fmd['emsc']
        year = fmd['year']
        dirname = 'globalmarine_{0}_{1}'.format(emsc, year).replace(' ', '')
        root = os.path.join(destdir, dirname)
        return root

    def gdal_options(self, md):
        # options to add metadata for the tiff file
        emsc = md['emsc']
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE']
        options += ['-mo', 'year_range={}'.format(md['year_range'])]
        options += ['-mo', 'year={}'.format(md['year'])]

        if emsc != 'current':
            options += ['-mo', 'emission_scenario={}'.format(emsc)]
        return options

    def filter_srcfiles(self, srcfile):
        """
        return False to skip this srcfile (zip file)
        """
        return srcfile.lower().endswith('.tif.zip')


def main():
    converter = MarineConverter()
    converter.main()


if __name__ == "__main__":
    main()
