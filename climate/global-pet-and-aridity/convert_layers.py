#!/usr/bin/env python
import os.path
import shutil
import tempfile
import zipfile
import copy

from concurrent import futures
from tqdm import tqdm

from data_conversion.utils import get_vsi_path
from data_conversion.converter import BaseConverter, run_gdal

LAYERINFO = {
    'Global PET - Annual.zip': { 
        'layerid': 'pet_he_yr', 
        'zipinfo': 'PET_he_annual/pet_he_yr/hdr.adf'
    },
    'Global Aridity - Annual.zip': {
        'layerid': 'ai_yr',
        'zipinfo': 'AI_annual/ai_yr/hdr.adf'
    }
}

class GlobalPetAridityConverter(BaseConverter):

    SCALES = {
        'ai_yr': 0.0001
    }

    def parse_zip_filename(self, srcfile):
        return {
            'year': '1975'
        }

    def parse_filename(self, filename):
        return {
            'layerid': LAYERINFO[os.path.basename(filename)]['layerid'],
            'year': '1975'
        }

    def skip_zipinfo(self, zipinfo):
        """
        return true to ignore this zipinfo entry
        """
        # default ignore directories
        if zipinfo.is_dir():
            return True
        # ignore non-anual files
        fname = os.path.basename(zipinfo.filename)
        return fname not in LAYERINFO.keys()

    def gdal_options(self, md):
        year = int(md['year'])

        options = ['-of', 'GTiff', '-co', 'TILED=YES']
        options += ['-co', 'COMPRESS=DEFLATE', '-norat']
        years = [year - 25, year + 25]
        options += ['-mo', 'year_range={}-{}'.format(years[0], years[1])]
        options += ['-mo', 'year={}'.format(year)]
        return options

    def target_dir(self, destdir, srcfile):
        md = self.parse_zip_filename(srcfile)
        dirname = 'pet_aridity_{}'.format(md['year'])
        root = os.path.join(destdir, dirname)
        return root

    def convert(self, srcfile, destdir):
        """convert .asc.gz files in folder to .tif in dest
        """
        parsed_zip_md = self.parse_zip_filename(srcfile)
        pool = futures.ProcessPoolExecutor(self.max_processes)
        results = []
        tempdirs = []
        try:
            with zipfile.ZipFile(srcfile) as srczip:
                for zipinfo in tqdm(srczip.filelist, desc="build jobs"):
                    if self.skip_zipinfo(zipinfo):
                        continue

                    # Extract the annual zip file
                    tempdir = tempfile.mkdtemp(prefix='pet_aridity')
                    tempdirs.append(tempdir)
                    srczip.extract(zipinfo, path=tempdir)

                    parsed_md = copy.deepcopy(parsed_zip_md)
                    parsed_md.update(
                        self.parse_filename(zipinfo.filename)
                    )
                    # apply scale and offset
                    if parsed_md['layerid'] in self.SCALES:
                        parsed_md['scale'] = self.SCALES[parsed_md['layerid']]
                    if parsed_md['layerid'] in self.OFFSETS:
                        parsed_md['offset'] = self.OFFSETS[parsed_md['layerid']]
                    destfilename = self.destfilename(destdir, parsed_md)
                    tmpsrczipfile = os.path.join(tempdir, zipinfo.filename)
                    srcurl = get_vsi_path(tmpsrczipfile, LAYERINFO[os.path.basename(zipinfo.filename)]['zipinfo'])
                    gdaloptions = self.gdal_options(parsed_md)
                    # output file name
                    destpath = os.path.join(destdir, destfilename)
                    # run gdal translate
                    cmd = ['gdal_translate']
                    cmd.extend(gdaloptions)
                    results.append(
                        pool.submit(run_gdal, cmd, srcurl, destpath, parsed_md)
                    )

            for result in tqdm(futures.as_completed(results),
                                    desc=os.path.basename(srcfile),
                                    total=len(results)):
                if result.exception():
                    tqdm.write("Job failed")
                    raise result.exception()
        finally:
            for d in tempdirs:
                shutil.rmtree(d)



def main():
    converter = GlobalPetAridityConverter()
    converter.main()


if __name__ == "__main__":
    main()
