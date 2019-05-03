#!/usr/bin/env python
import os.path
from concurrent import futures
import copy
import tempfile
import zipfile

from osgeo import gdal
from tqdm import tqdm

from data_conversion.converter import BaseConverter
from data_conversion.coverage import gen_coverage_uuid
from data_conversion.vocabs import RESOLUTIONS, collection_by_id
from data_conversion.utils import get_vsi_path, retry_run_cmd
from data_conversion.vocabs import VAR_DEFS, PREDICTORS


# map source file id's to our idea of RCP id's
EMSC_MAP = {
    'RCP3PD': 'RCP2.6',
    'RCP6': 'RCP6.0',
    'RCP45': 'RCP4.5',
    'RCP85': 'RCP8.5',
}

GCM_MAP = {
    'ac': 'ACCESS1-0',  # non-commercial
    'bc': 'BCC-CSM-1',
    'cc': 'CCSM4',
    'ce': 'CESM1-CAM5-1-FV2',
    'cn': 'CNRM-CM5',
    'gf': 'GFDL-CM3',
    'gd': 'GFDL-ESM2G',
    'gs': 'GISS-E2-R',
    'hd': 'HadGEM2-AO',
    'hg': 'HadGEM2-CC',
    'he': 'HadGEM2-ES',
    'in': 'INMCM4',
    'ip': 'IPSL-CM5A-LR',
    'mi': 'MIROC-ESM-CHEM',  # non-commercial
    'mr': 'MIROC-ESM',  # non-commercial
    'mc': 'MIROC5',  # non-commercial
    'mp': 'MPI-ESM-LR',
    'mg': 'MRI-CGCM3',
    'no': 'NorESM1-M',
}

SCALES = {
    'tmean': 0.1,
    'tmin': 0.1,
    'tmax': 0.1,
    'bioclim_01': 0.1,
    'bioclim_02': 0.1,
    # 'bioclim_03': 0.1,
    'bioclim_04': 0.1,
    'bioclim_05': 0.1,
    'bioclim_06': 0.1,
    'bioclim_07': 0.1,
    'bioclim_08': 0.1,
    'bioclim_09': 0.1,
    'bioclim_10': 0.1,
    'bioclim_11': 0.1,
}

# Worldclim current seems to be slightly off, we use this map to adjust it.
GEO_TRANSFORM_PATCH = {
    '10m': (-180.0, 0.16666666666666666, 0.0, 90.0, 0.0, -0.16666666666666666),
    '2-5m': (-180.0, 0.041666666666667, 0.0, 90.0, 0.0, -0.041666666666667),
    '5m': (-180.0, 0.083333333333333, 0.0, 90.0, 0.0, -0.083333333333333),
    '30s': (-180.0, 0.008333333333333, 0.0, 90.0, 0.0, -0.008333333333333),
}


def run_gdal(cmd, infile, outfile, md):
    """
    Run gdal_translate in sub process.
    """
    _, tfname = tempfile.mkstemp(suffix='.tif')
    try:
        retry_run_cmd(cmd + [infile, tfname])
        # add band metadata
        ds = gdal.Open(tfname, gdal.GA_Update)
        # Patch GeoTransform ... at least worldclim current data is
        #                        slightly off
        ds.SetGeoTransform(GEO_TRANSFORM_PATCH[md.get('resolution')])
        # For some reason we have to flust the changes to geo transform
        # immediately otherwise gdal forgets about it?
        # TODO: check if setting ds = None fixes this as well?
        ds.FlushCache()

        # adapt layerid from zip file to specific layer inside zip
        if md.get('month'):
            ds.SetMetadataItem('month', str(md.get('month')))

        band = ds.GetRasterBand(1)
        # ensure band stats
        band.ComputeStatistics(False)
        layerid = md['layerid']
        for key, value in VAR_DEFS[layerid].items():
            band.SetMetadataItem(key, value)
        # just for completeness
        band.SetUnitType(VAR_DEFS[layerid]['units'])
        band.SetScale(md.get('scale', 1.0))
        band.SetOffset(md.get('offset', 0.0))
        ds.FlushCache()
        # build command
        cmd = [
            'gdal_translate',
            '-of', 'GTiff',
            '-co', 'TILED=yes',
            '-co', 'COPY_SRC_OVERVIEWS=YES',
            '-co', 'COMPRESS=DEFLATE',
            '-co', 'PREDICTOR={}'.format(PREDICTORS[band.DataType]),
        ]
        # check CRS
        # Worldclim future datasets have incomplete projection information
        # let's force it to a known proj info anyway
        cmd.extend(['-a_srs', 'EPSG:4326'])

        # close dataset
        del band
        del ds
        # gdal_translate once more to cloud optimise geotiff
        cmd.extend([tfname, outfile])
        retry_run_cmd(cmd)
    except Exception as e:
        print('Error:', e)
        raise e
    finally:
        os.remove(tfname)



class WorldClimConverter(BaseConverter):

    def parse_zip_filename(self, srcfile):
        """
        Parse filename of the format 1km/RCP85_ncar-pcm1_2015.zip to get emsc and
        gcm and year and resolution
        """
        basename, _ = os.path.splitext(os.path.basename(srcfile))
        basedir = os.path.basename(os.path.dirname(srcfile))
        gcm = None
        if basedir == 'current':
            # it's a current file ... type_ = 'esri'
            var, res, type_ = basename.split('_')
            emsc = gcm = 'current'
            year = 1975
        else:
            # it's future .. basedir is resolution
            gcm, emsc, var, year = re.findall(r'\w{2}', basename)
            res = basedir.replace(".", '-')
            type_ = 'tif'
            year = 2000 + int(year)
            emsc = EMSC_MAP[emsc]
            gcm = GCM_MAP[gcm]
        return {
            'emsc': emsc,
            'gcm': gcm,
            'resolution': res,
            'year': year,
            'type': type_
        }

    def parse_filename(self, filename):
        """
        parse a full path within a zip file and return a dict
        with informatn extracted from path and filename
        """
        basedir = os.path.basename(os.path.dirname(filename))
        lzid = basedir.split('_')[0]
        month = None
        if lzid in ('prec', 'tmin', 'tmax', 'tmean'):
            # current other
            layerid = lzid
            month = int(basedir.split('_')[1])
        elif lzid == 'alt':
            # current alt
            layerid = lzid
        elif lzid == 'bio':
            # current dataset
            layerid = 'bioclim_{:02d}'.format(int(basedir.split('_')[1]))
        elif lzid == 'bi':
            # future
            # last one or two digits before '.tif' are bioclim number
            layerid = 'bioclim_{:02d}'.format(int(basedir[8:-4]))
        elif lzid == 'pr':
            # future
            layerid = 'prec'
            month = int(basedir[8:-4])
        elif lzid == 'tn':
            # future
            layerid = 'tmin'
            month = int(basedir[8:-4])
        elif lzid == 'tx':
            # future
            layerid = 'tmax'
            month = int(basedir[8:-4])
        else:
            raise Exception('Unknown lzid {}'.format(lzid))
        md = { 'layerid': layerid }
        if month is not None:
            md['month'] = month
        return md

    def skip_zipinfo(self, zipinfo):
        """
        return true to ignore this zipinfo entry
        """
        # default ignore directories
        if zipinfo.is_dir():
            return True
        # only want tif, hdr.adf file
        _, ext = os.path.splitext(zipinfo.filename)
        if ext not in ('.tif', '.adf'):
            return True
        if ext == '.adf' and os.path.basename(zipinfo.filename) != 'hdr.adf':
            return True
        return False

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
            # Altitude don't have year
            if md['layerid'] != 'alt':
                options += ['-mo', 'year_range={}-{}'.format(year - 15, year + 15)]
                options += ['-mo', 'year={}'.format(year)]
        else:
            options += ['-mo', 'emission_scenario={}'.format(emsc)]
            options += ['-mo', 'general_circulation_models={}'.format(gcm.upper())]
            options += ['-mo', 'year_range={}-{}'.format(year - 9, year + 10)]
            options += ['-mo', 'year={}'.format(year)]
        options += ['-mo', 'version=1.4']
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

    def convert(self, srcfile, destdir):
        """convert .asc.gz files in folder to .tif in dest
        """
        parsed_zip_md = self.parse_zip_filename(srcfile)
        pool = futures.ProcessPoolExecutor(3)
        results = []
        with zipfile.ZipFile(srcfile) as srczip:
            for zipinfo in tqdm(srczip.filelist, desc="build jobs"):
                if self.skip_zipinfo(zipinfo):
                    continue

                parsed_md = copy.copy(parsed_zip_md)
                parsed_md.update(
                    self.parse_filename(zipinfo.filename)
                )
                # apply scale and offset
                if parsed_md['layerid'] in self.SCALES:
                    parsed_md['scale'] = self.SCALES[parsed_md['layerid']]
                if parsed_md['layerid'] in self.OFFSETS:
                    parsed_md['offset'] = self.OFFSETS[parsed_md['layerid']]
                destfilename = self.destfilename(destdir, parsed_md)
                srcurl = get_vsi_path(srcfile, zipinfo.filename)
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



def main():
    converter = WorldClimConverter()
    converter.main()


if __name__ == "__main__":
    main()
