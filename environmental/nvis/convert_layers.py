#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseConverter


LAYERINFO = {
    # NVIS Australian vegetation group
    # source directory file, ('source fragment', layer_id)
    'GRID_NVIS4_2_AUST_EXT_MVG': ('aus4_2e_mvg', 'amvg'),
    'GRID_NVIS4_2_AUST_PRE_MVG': ('aus4_2p_mvg', 'amvg-1975')
}


class NVISConverter(BaseConverter):

    def parse_zip_filename(self, srcfile):
        basename = os.path.basename(srcfile)
        fname, _ = os.path.splitext(basename)
        # layerid, year
        return {
           'layerid': LAYERINFO[fname][1],
            #'year': 2016,
            'version': '4.2',
            'fragment': LAYERINFO[fname][0],
        }

    # get layer id from filename within zip file
    def parse_filename(self, filename):
        layerid = os.path.basename(os.path.dirname(filename))
        return {
            'layerid': 'amvg',
        }

    def destfilename(self, destdir, md):
        """
        generate file name for output tif file.
        """
        return (
            os.path.basename(destdir) +
            '_' +
            md['fragment'] +
            '_' +
            md['layerid'].replace('_', '-') +
            '.tif'
        )


    def target_dir(self, destdir, srcfile):
        root = os.path.join(destdir, 'nvis-2016-90m')
        return root

    def gdal_options(self, md):
        # options to add metadata for the tiff file
        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE']
        options += ['-mo', 'version={}'.format(md['version'])]
        options += ['--config', 'GDAL_PAM_MODE', 'PAM']
        #options += ['-mo', 'year_range={}-{}'.format(year, year)]
        #options += ['-mo', 'year={}'.format(year)]
        return options

    def skip_zipinfo(self, zipinfo):
        if zipinfo.is_dir():
            return True
        if os.path.basename(zipinfo.filename) != 'w001001.adf':
            # skip non data dirs
            return True
        return False


def main():
    converter = NVISConverter()
    converter.main()


if __name__ == "__main__":
    main()
