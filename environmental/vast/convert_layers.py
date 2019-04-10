#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseConverter


LAYERINFO = {
    'vast-data': ('vastgridv2_1k', 2008)
}

class VASTConverter(BaseConverter):

    def parse_zip_filename(self, srcfile):
        basename = os.path.basename(srcfile)
        fname, _ = os.path.splitext(basename)
        # layerid, year
        return {
            'layerid': LAYERINFO[fname.lower()][0],
            'year': LAYERINFO[fname.lower()][1],
        }

    def target_dir(self, destdir, srcfile):
        root = os.path.join(destdir, 'vast-2008-1km')
        return root

    def gdal_options(self, md):
        # options to add metadata for the tiff file
        year = md['year']
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE']
        options += ['-mo', 'year_range={}-{}'.format(year, year)]
        options += ['-mo', 'year={}'.format(year)]
        return options

    def skip_zipinfo(self, zipinfo):
        if not zipinfo.filename.endswith('/hdr.adf'):
            # skip non data dirs
            return True
        return False

def main():
    converter = VASTConverter()
    converter.main()


if __name__ == "__main__":
    main()
