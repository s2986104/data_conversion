#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseConverter


class TASCLimConverter(BaseConverter):

    def parse_zip_filename(self, srcfile):
        basename, _ = os.path.splitext(os.path.basename(srcfile))
        parts = basename.split('_')
        emsc = 'SRES-' + parts[1]
        # To remove decimal point in CM2.0
        if len(parts) > 3:
            parts[3] = parts[3].replace('.', '')
        gcm = '-'.join(parts[2:]) 
        return {
            'gcm': gcm,
            'emsc': emsc,
        }

    def parse_filename(self, filename):
        parts = os.path.splitext(os.path.basename(filename))[0].split('_')
        if len(parts) > 4:
            # special case for MIROC3.2_MEDRES which should be MIROC3.2-MEDRES
            _, _, _, layerid, year = parts
        else:
            _, _, layerid, year = parts
        return {
            'layerid': 'bioclim_{:02d}'.format(int(layerid)),
            'year': int(year)
        }

    def gdal_options(self, md):
        gcm = md['gcm']
        emsc = md['emsc']
        year = int(md['year'])

        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE', '-norat']
        # worldclim future spans 30 years
        years = [year - 14, year + 15]
        options += ['-mo', 'emission_scenario={}'.format(emsc)]
        options += ['-mo', 'general_circulation_models={}'.format(gcm.upper())]
        options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
        options += ['-mo', 'year={}'.format(year)]
        return options

    def destfilename(self, destdir, md):
        return '_'.join((
            os.path.basename(destdir),
            str(md['year']),
            md['layerid'].replace('_', '-'),
            '.tif'
        ))

    def target_dir(self, destdir, srcfile):
        md = self.parse_zip_filename(srcfile)
        dirname = '_'.join(('tasclim', md['emsc'], md['gcm']))
        root = os.path.join(destdir, dirname)
        return root

    def filter_srcfiles(self, srcfile):
        return (
            'Metadata' not in srcfile and
            'GCM_MEAN' not in srcfile  # skip files which are mean over all GCMs
        )

def main():
    converter = TASCLimConverter()
    converter.main()


if __name__ == "__main__":
    main()
