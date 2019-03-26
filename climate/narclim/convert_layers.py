#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseConverter


class NaRCLIMConverter(BaseConverter):

    SCALES = {
        'bioclim_15': 0.1
    }


    def parse_zip_filename(self, srcfile):
        basename, _ = os.path.splitext(os.path.basename(srcfile))
        basedir = os.path.basename(os.path.dirname(srcfile))
        res = '36' if basedir == 'NaRCLIM_1km' else '9'
        parts = basename.split('_')
        if parts[1] == 'baseline':
            # it's a current file
            gcm = 'current'
            emsc = 'SRES-A2'
            rcm = None
            year = int('2000')
            extent = '{}-{}'.format(parts[2], parts[3])
        else:
            # it's future .. basedir is resolution
            _, _, year, gcm, rcm = parts
            emsc = 'SRES-A2'
            extent = None
        return {
            'gcm': gcm,
            'rcm': rcm,
            'emsc': emsc,
            'year': year,
            'resolution': res,
            'extent': extent,
        }

    def parse_filename(self, filename):
        fname = os.path.splitext(os.path.basename(filename))[0]
        _, _, layerid = fname.split('_')
        return {
            'layerid': 'bioclim_{:02d}'.format(int(layerid))
        }

    def gdal_options(self, md):
        gcm = md['gcm']
        emsc = md['emsc']
        rcm = md['rcm']
        year = int(md['year'])
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE', '-norat']
        options += ['-mo', 'year_range={}-{}'.format(year-10, year+9)]
        options += ['-mo', 'year={}'.format(year)]
        if emsc and emsc != 'current':
            options += ['-mo', 'emission_scenario={}'.format(emsc)]
        if gcm != 'current':
            options += ['-mo', 'general_circulation_models={}'.format(gcm.upper())]
        if rcm:
            options += ['-mo', 'regional_climate_model={}'.format(rcm)]
        return options

    def target_dir(self, destdir, srcfile):
        fmd = self.parse_zip_filename(srcfile)
        gcm = fmd['gcm']
        emsc = fmd['emsc']
        year = fmd['year']
        rcm = fmd['rcm']
        resolution = fmd['resolution']
        extent = fmd['extent']
        
        if gcm == 'current':
            dirname = '_'.join(('narclim', 'current', extent, emsc, resolution))
        else:
            dirname = '_'.join(('narclim', emsc, gcm, rcm, str(year), resolution))
        root = os.path.join(destdir, resolution, dirname)
        return root

    def destfilename(self, destdir, md):
        return '{}.tif'.format(md['layerid']) 

    def filter_srcfiles(self, srcfile):
        return 'Aus' not in srcfile and 'Metadata' not in srcfile


def main():
    converter = NaRCLIMConverter()
    converter.main()
    

if __name__ == "__main__":
    main()
