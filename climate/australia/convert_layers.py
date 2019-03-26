#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseConverter

# map source file id's to our idea of RCP id's
EMSC_MAP = {
    'RCP3PD': 'RCP2.6',
    'RCP6': 'RCP6.0',
    'RCP45': 'RCP4.5',
    'RCP85': 'RCP8.5',
}


class AustraliaConverter(BaseConverter):

    def parse_zip_filename(self, srcfile):
        """
        Parse filename of the format 1km/RCP85_ncar-pcm1_2015.zip to get emsc and
        gcm and year and resolution
        """
        # this should always parse source file ... we know the dest file anyway
        resolution = os.path.basename(os.path.dirname(srcfile))
    
        basename = os.path.basename(srcfile)
        basename, _ = os.path.splitext(basename)
        parts = basename.split('_')

        if parts[0].startswith('current'):
            emsc, gcm, year = 'current', 'current', '1976-2005'
        else:
            emsc, gcm, year = parts
        return {
            'resolution': resolution,
            'emsc': EMSC_MAP.get(emsc, emsc),
            'gcm': gcm,
            'year': year
        }

    def parse_filename(self, filename):
        layerid = os.path.splitext(os.path.basename(filename))[0]
        return {'layerid': layerid}


    def gdal_options(self, md):
        """
        options to add metadata for the tiff file
        """
        emsc = md['emsc']
        gcm = md['gcm']
        year = md['year']

        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE', '-norat']
        if emsc == 'current':
            years = [int(x) for x in year.split('-')]
            options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
            options += [
                '-mo', 'year={}'.format(
                    int(((years[1] - years[0] - 1) / 2) + years[0])
                )
            ]
        else:
            year = int(year)
            years = [year - 4, year + 5]
            options += ['-mo', 'emission_scenario={}'.format(emsc)]
            options += ['-mo', 'general_circulation_models={}'.format(gcm.upper())]
            options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
            options += ['-mo', 'year={}'.format(year)]
        return options

    def target_dir(self, destdir, srcfile):
        fmd = self.parse_zip_filename(srcfile)
        res = fmd['resolution']
        emsc = fmd['emsc']
        gcm = fmd['gcm']
        year = fmd['year']
        if emsc == 'current':
            dirname = 'current_{year}'.format(year=year)
        else:
            dirname = '{0}_{1}_{2}'.format(emsc, gcm, year).replace(' ', '')
        root = os.path.join(destdir, res, dirname)
        return root


def main():
    converter = AustraliaConverter()
    converter.main()


if __name__ == "__main__":
    main()
