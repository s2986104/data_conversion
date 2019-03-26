#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseConverter


# map source file id's to our idea of RCP id's
EMSC_MAP = {
    'A1B': 'SRES-A1B',
    'A2': 'SRES-A2'
}


class CLIMONDConverter(BaseConverter):

    SCALES = {
        'bioclim_04': 100.0
    }

    def parse_zip_filename(self, srcfile):
        basename = os.path.basename(srcfile)
        basename, _ = os.path.splitext(basename)
        parts = basename.split('_')

        if parts[1] == 'CURRENT':
            emsc, gcm = 'current', 'current'
        else:
            emsc, gcm = parts[1], parts[2]
        return {
            'emsc': EMSC_MAP.get(emsc, emsc),
            'gcm': gcm
        }

    def parse_filename(self, filename):
        # current dataset filename has 2 parts only
        parts = os.path.splitext(os.path.basename(filename))[0].split('_')
        layerid = 'bioclim_{:02d}'.format(int(parts[1]) if len(parts) == 2 else int(parts[3]))
        year = '1975' if len(parts) == 2 else parts[4]
        return {
            'layerid': layerid,
            'year': year,
        }

    def gdal_options(self, md):
        emsc = md['emsc']
        gcm = md['gcm']
        year = int(md['year'])

        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE', '-norat']

        years = [year - 14, year + 15]
        options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
        options += ['-mo', 'year={}'.format(year)]

        if emsc != 'current':
            options += ['-mo', 'emission_scenario={}'.format(emsc)]
            options += ['-mo', 'general_circulation_models={}'.format(gcm.upper())]
        return options


    def target_dir(self, destdir, srcfile):
        fmd = self.parse_zip_filename(srcfile)
        emsc = fmd['emsc']
        gcm = fmd['gcm']
        if emsc == 'current':
            dirname = 'current'
        else:
            dirname = '{0}_{1}'.format(emsc, gcm).replace(' ', '')
        root = os.path.join(destdir, dirname)
        return root

    def filter_srcfiles(self, srcfile):
        return 'METADATA' not in srcfile


def main():
    converter = CLIMONDConverter()
    converter.main()


if __name__ == "__main__":
    main()
