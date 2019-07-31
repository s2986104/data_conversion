#!/usr/bin/env python
import os
import os.path
from concurrent import futures
import copy
import re
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
    '26': 'RCP2.6',
    '45': 'RCP4.5',
    '60': 'RCP6.0',
    '85': 'RCP8.5',
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

VAR_MAP = {
    'alt': 'altitude',
    'prec': 'precipitation_mean',
    'tmax': 'temperature_max',
    'tmin': 'temperature_min',
    'tmean': 'temperature_mean',
    'srad': 'solar_radiation',
    'vapr': 'vapor_pressure',
    'wind': 'wind_speed',
}

REV_VAR_MAP = {
    'altitude': 'alt',
    'precipitation_mean': 'prec',
    'temperature_max': 'tmax',
    'temperature_min': 'tmin',
    'temperature_mean': 'tmean',
    'solar_radiation': 'srad',
    'vapor_pressure': 'vapr',
    'wind_speed': 'wind',
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
    tfd, tfname = tempfile.mkstemp(
        prefix='run_{}_'.format(os.path.splitext(os.path.basename(infile))[0]),
        suffix='.tif',
    )
    os.close(tfd)
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

        band = ds.GetRasterBand(1)
        # ensure band stats
        band.ComputeStatistics(False)
        layerid = md['layerid']
        for key in ('standard_name', 'long_name', 'measure_type'):
            band.SetMetadataItem(key, VAR_DEFS[layerid][key])
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

    SCALES = {
        'temperature_mean': 0.1,
        'temperature_min': 0.1,
        'temperature_max': 0.1,
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


    def parse_zip_filename(self, srcfile):
        """
        Parse filename of the format 1km/RCP85_ncar-pcm1_2015.zip to get emsc and
        gcm and year and resolution
        """
        parts = srcfile.split(os.path.sep)
        basename, _ = os.path.splitext(parts[-1])
        # get other parts from path
        version, period, resolution = parts[-4:-1]
        gcm = None
        # file name scheme different per version
        if version == 'v1.4':
            if period == 'current':
                # it's a current file ... type_ = 'esri'
                var, _, type_ = basename.split('_')
                emsc = gcm = 'current'
                year = 1975
            else:
                # it's future .. basedir is resolution
                gcm, emsc, var, year = re.findall(r'\w{2}', basename)
                type_ = 'tif'
                year = 2000 + int(year)
                emsc = EMSC_MAP[emsc]
                gcm = GCM_MAP[gcm]
        else:
            if period == 'current':
                _, _, var = basename.split('_')
                emsc = gcm = 'current'
                year = 1985
                type_ = 'tif'
            else:
                raise Exception('v2.0 future not implemented')

        return {
            'emsc': emsc,
            'gcm': gcm,
            'resolution': resolution.replace('.', '-'),
            'year': year,
            'version': version,
            'type': type_
        }

    def parse_filename(self, filename):
        """
        parse a full path within a zip file and return a dict
        with informatn extracted from path and filename
        """
        if filename.endswith('.adf'):
            # 1.4 current is in esri format
            # things are in folders
            basedir = os.path.basename(os.path.dirname(filename))
            # subid is either month or bioclim layer num
            lzid, subid = basedir.split('_')
        else:
            # 1.4 future in geotiff
            match = re.match(r'(?P<gcm>\w{2})(?P<emsc>\d{2})(?P<var>\w{2})(?P<year>\d{2})(?P<sub>\d{1,2}).tif', filename)
            if match:
                lzid = match.group('var')
                subid = match.group('sub')
            else:
                # 2.0 current in geotiff
                match = re.match(r'wc2.0_(?P<res>.+)_(?P<var>.+)_(?P<sub>\d{2}).tif', filename)
                if match:
                    if match.group('res') == 'bio':
                        # bio is swapped around
                        lzid = match.group('res')
                    else:
                        lzid = match.group('var')
                    subid = match.group('sub')
                else:
                    raise Exception('No regexp matched.')

        month = None
        if lzid in ('prec', 'tmin', 'tmax', 'tmean', 'srad', 'vapr', 'wind'):
            # current other
            layerid = lzid
            month = int(subid)
        elif lzid in ('tavg'):
            # current 2.0
            layerid = 'tmean'
            month = int(subid)
        elif lzid == 'alt':
            # current alt
            layerid = lzid
        elif lzid == 'bio':
            # current dataset
            layerid = 'bioclim_{:02d}'.format(int(subid))
        elif lzid == 'bi':
            # future
            # last one or two digits before '.tif' are bioclim number
            layerid = 'bioclim_{:02d}'.format(int(subid))
        elif lzid == 'pr':
            # future
            layerid = 'prec'
            month = int(subid)
        elif lzid == 'tn':
            # future
            layerid = 'tmin'
            month = int(subid)
        elif lzid == 'tx':
            # future
            layerid = 'tmax'
            month = int(subid)
        else:
            raise Exception('Unknown lzid {}'.format(lzid))
        if 'layerid' in VAR_MAP:
            md = {'layerid': VAR_MAP[layerid]}
        else:
            md = {'layerid': layerid}
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
            if md['layerid'] != 'altitude':
                options += ['-mo', 'year_range={}-{}'.format(year - 15, year + 15)]
                options += ['-mo', 'year={}'.format(year)]
        else:
            options += ['-mo', 'emission_scenario={}'.format(emsc)]
            options += ['-mo', 'general_circulation_models={}'.format(gcm.upper())]
            options += ['-mo', 'year_range={}-{}'.format(year - 9, year + 10)]
            options += ['-mo', 'year={}'.format(year)]

        # add month 
        if md.get('month'):
            options += ['-mo', 'month={}'.format(md.get('month'))]

        options += ['-mo', 'version={}'.format(md['version'])]
        return options

    def target_dir(self, destdir, srcfile):
        # srcfile ... src zip file
        fmd = self.parse_zip_filename(srcfile)
        res = fmd['resolution']
        emsc = fmd['emsc']
        gcm = fmd['gcm']
        year = fmd['year']
        version = fmd['version']
        if emsc == 'current':
            dirname = '{version}_current_{year}'.format(version=version, year=year)
        else:
            dirname = '{0}_{1}_{2}_{3}'.format(version, emsc, gcm, year).replace(' ', '')
        root = os.path.join(destdir, res, dirname)
        return root

    def destfilename(self, destdir, md):
        if 'month' in md:
            # include month in filename
            if md['layerid'] in REV_VAR_MAP:
                lzid = md['layerid']
            else:
                lzid = layerid
            return (
                os.path.basename(destdir) +
                '_' +
                lzid.replace('_', '-') +
                '_' +
                '{:02d}'.format(md['month']) +
                '.tif'
            )
        return (
            os.path.basename(destdir) +
            '_' +
            lzid.replace('_', '-') +
            '.tif'
        )

    def filter_srcfiles(self, srcfile):
        if self.opts.filter:
            return self.opts.filter.search(srcfile)
        return True

    def update_scale_offset(self, md):
        if md['version'] == 'v1.4':
            if md['layerid'] in self.SCALES:
                parsed_md['scale'] = self.SCALES[md['layerid']]
            if md['layerid'] in self.OFFSETS:
                parsed_md['offset'] = self.OFFSETS[md['layerid']]

    def get_argument_parser(self):
        parser = super().get_argument_parser()
        # TODO: add filter params
        parser.add_argument(
            '--filter', action='store',
            help='regexp to filter srcfiles. applied on full path'
        )
        return parser

    def parse_args(Self):
        import re
        opts = super().parse_args()
        if opts.filter:
            opts.filter = re.compile(opts.filter)
        return opts


def main():
    converter = WorldClimConverter()
    converter.main()


if __name__ == "__main__":
    main()
