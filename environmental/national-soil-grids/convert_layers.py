#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseConverter


LAYERINFO = {
    'clay30': ('clay30', 2011),
    'asc': ('asc', 2012),
    'pawc_1m': ('pawc_1m', 2014),
    'ph': ('ph_0_30', 2014),
    'bd30': ('bd30', 2011)
}


class NSGConverter(BaseConverter):

    def parse_zip_filename(self, srcfile):
        basename = os.path.basename(srcfile)
        fname, _ = os.path.splitext(basename)
        # layerid, year
        return {
            'layerid': LAYERINFO[fname.lower()][0],
            'year': LAYERINFO[fname.lower()][1],
        }

    # get layer id from filename within zip file
    def parse_filename(self, filename):
        layerid = os.path.basename(os.path.dirname(filename))
        return {
            'layerid': layerid,
        }

    def target_dir(self, destdir, srcfile):
        root = os.path.join(destdir, 'nsg-2011-250m')
        return root

    def gdal_options(self, md):
        # options to add metadata for the tiff file
        year = md['year']
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE', '--config']
        options += ['-mo', 'year_range={}-{}'.format(year, year)]
        options += ['-mo', 'year={}'.format(year)]
        return options

    def skip_zipinfo(self, zipinfo):
        if not zipinfo.filename.endswith('/hdr.adf'):
            # skip non data dirs
            return True
        # also skip all hdr.adf inside a folder ending in _src
        layer = os.path.dirname(zipinfo.filename)
        if layer.endswith('_src'):
            return True
        return False


def main():
    converter = NSGConverter()
    converter.main()


if __name__ == "__main__":
    main()
