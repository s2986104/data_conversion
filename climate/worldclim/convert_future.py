import re
import sys
import glob
import json
import os
import os.path
import zipfile
import itertools
import tempfile
import shutil

JSON_TEMPLATE = 'worldclim.template.json'
TITLE_TEMPLATE = u'WorldClim Future Projection using {gcm} {rcp} at {res} ({year})'

TMPDIR = os.getenv("BCCVL_TMP", "/mnt/playground/")

## Lookups for metadata construction
#  Details taken from:
#    http://www.worldclim.org/cmip5_10m
#    http://www.worldclim.org/formats

RESOLUTION_MAP = {
    '30s': 'Resolution30s',
    '2.5m': 'Resolution2_5m',
    '5m': 'Resolution5m',
    '10m': 'Resolution10m',
}

# global climate models (GCMs)
GCM_MAP = {
    'ac': 'ACCESS1-0',
    'bc': 'BCC-CSM1-1',
    'cc': 'CCSM4',
    'ce': 'CESM1-CAM5-1-FV2',
    'cn': 'CNRM-CM5',
    'gf': 'GFDL-CM3',
    'gd': 'GFDL-ESM2G',
    'gs': 'GISS-E2-R',
    'hd': 'HadGEM2-A0',
    'hg': 'HadGEM2-CC',
    'he': 'HadGEM2-ES',
    'in': 'INMCM4',
    'ip': 'IPSL-CM5A-LR',
    'mi': 'MIROC-ESM-CHEM',
    'mr': 'MIROC-ESM',
    'mc': 'MIROC5',
    'mp': 'MPI-ESM-LR',
    'mg': 'MRI-CGCM3',
    'no': 'NorESM1-M',
}

# representative concentration pathways (RCPs)
RCP_MAP = {
    '26': 'RCP3PD',
    '45': 'RCP4.5',
    '60': 'RCP6',
    '85': 'RCP8.5',
}

#VARIABLE_MAP = {
#    'tn': 'Monthly Average Minimum Temperature',
#    'tx': 'Monthly Average Maximum Temperature',
#    'pr': 'Monthly Total Precipitation',
#    'bi': 'Bioclim',
#}

LAYER_TYPE_MAP = {
   'tn': 'tmin',
   'tx': 'tmax',
   'pr': 'prec',
   'bi': 'bioclim',
}


YEAR_MAP = {
    '50': '2050',
    '70': '2070',
}

##

GEOTIFF_PATTERN = re.compile(
    r"""
    (?P<gcm>..)                 # GCM
    (?P<rcp>[0-9][0-9])         # RCP
    (?P<layer_type>..)          # Layer type
    (?P<year>[0-9][0-9])        # Year
    (?P<layer_num>[0-9][0-9]?)  # Layer no.
    """,
    re.VERBOSE
)

def convert_filename(filename):
    geotiff_info = GEOTIFF_PATTERN.match(filename).groupdict()
    return "{0}_{1:02d}.tif".format(LAYER_TYPE_MAP[geotiff_info['layer_type']], int(geotiff_info['layer_num']))


def potential_converts(source):
    potentials = itertools.product(
        GCM_MAP, RCP_MAP, YEAR_MAP, RESOLUTION_MAP
    )
    for gcm, rcp, year, res in potentials:
        source_filter = '{gcm}{rcp}*{year}.zip'.format(**locals())
        source_path = os.path.join(source, res, source_filter)
        source_files = glob.glob(source_path)
        if source_files:
            yield gcm, rcp, year, res, source_files

def convert_geotiff_temperature(itemname, file_content, scale):
    tmpdir = tempfile.mkdtemp(dir=TMPDIR)
    infile = os.path.join(tmpdir, "infile.tif")
    outfile = os.path.join(tmpdir, "outfile.tif")
    with open(infile, 'wb') as f:
        f.write(file_content)
    print "Changing temperature representation for {}".format(itemname)
    # No data is set to -9999 is used to be consistent with other datasets.
    cmd = 'gdal_calc.py -A {infile} --calc="A*{scale}" --co="COMPRESS=LZW" --NoDataValue=-9999 --co="TILED=YES" --outfile {outfile} --type "Float32"'.format(
        **locals())
    ret = os.system(cmd)
    if ret != 0:
        raise Exception("COMMAND '{}' failed.".format(cmd))
    with open(outfile, 'rb') as f:
        new_file_content = f.read()
    shutil.rmtree(tmpdir)
    return new_file_content


def fix_geotiff(file_content):
    """ For some reason the original GeoTIFFs cause issues with gdal calc (file sizes become enourmous after conversion with gdal_calc).
        This function uses GDAL Translate to convert the files into new GeoTIFFs that for some reason don't have this problem
        despite appearing to have the same content.
    """
    print "Running gdal_translate to fix GeoTIFF"
    tmpdir = tempfile.mkdtemp(dir=TMPDIR)
    infile = os.path.join(tmpdir, "infile.tif")
    outfile = os.path.join(tmpdir, "outfile.tif")
    with open(infile, 'wb') as f:
        f.write(file_content)
    ret = os.system(
        'gdal_translate -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" {0} {1}'.format(infile, outfile)
    )
    if ret != 0:
        raise Exception("can't gdal_translate {0} ({1})".format(srcfile, ret))
    with open(outfile, 'rb') as f:
        new_file_content = f.read()
    shutil.rmtree(tmpdir)
    return new_file_content


def get_scale_factor(itemname):
    # tn, tx and a subset of bioclim are temperature layers
    info = GEOTIFF_PATTERN.match(itemname).groupdict()
    bioclim_temp_layers = ['1', '2', '5', '6', '7', '8', '9', '10', '11']
    if info['layer_type'] in ['tn', 'tx']:
        return '0.1'
    elif info['layer_type'] == 'bi' and info['layer_num'] in bioclim_temp_layers:
        return '0.1'
    elif info['layer_type'] == 'bi' and info['layer_num'] in ['3']:
        return '0.01'
    elif info['layer_type'] == 'bi' and info['layer_num'] in ['4']:
        return '0.001'
    else:
        return None


def get_geotiff_str(itemname, file_content):
    fixed_geotiff = fix_geotiff(file_content)
    scale = get_scale_factor(itemname)
    if scale:
        return convert_geotiff_temperature(itemname, fixed_geotiff, scale)
    else:
        return fixed_geotiff



def add_source_files(destzip, destname, filenames):
    for filename in filenames:
        try:
            srczip = zipfile.ZipFile(filename, 'r')
        except zipfile.BadZipfile:
            print "Unable to handle '{}' as zip".format(filename)
            continue
        for itemname in (item.filename for item in srczip.filelist):
            geotiff_str = get_geotiff_str(itemname, srczip.read(itemname))
            destzip.writestr(
                os.path.join(destname, 'data', convert_filename(itemname)), geotiff_str
            )


def add_metadata(destzip, destname, metadata):
    destzip.writestr(
        os.path.join(destname, 'bccvl', 'metadata.json'), metadata
    )



def create_metadata_json(destfile, gcm, rcp, year, res, files):
    with open(JSON_TEMPLATE, 'r') as template:
        meta = json.load(template)
        meta[u'title'] = TITLE_TEMPLATE.format(
            gcm = GCM_MAP[gcm],
            rcp = RCP_MAP[rcp],
            year = YEAR_MAP[year],
            res = RESOLUTION_MAP[res],
        )
        meta[u'temporal_coverage'][u'start'] = YEAR_MAP[year]
        meta[u'temporal_coverage'][u'end'] = YEAR_MAP[year]
        meta[u'genre'] = u'FutureClimate'
        meta[u'resolution'] = RESOLUTION_MAP[res]
        meta[u'gcm'] = GCM_MAP[gcm]
        meta[u'emsc'] = RCP_MAP[rcp]
        meta[u'files'] = {}
        for fname in files:
            m = GEOTIFF_PATTERN.match(fname)
            fpath = os.path.join(destfile, 'data', fname)
            if m:
                meta[u'files'][fpath] = {
                    u'layer': layer_id(m.group('layer_type'), m.group('layer_num'))
                }
        return json.dumps(meta, indent=4)

def layer_id(layer_type, layer_num):
    layer_prefix = 'B' if layer_type == 'bi' else layer_type.upper()
    if layer_prefix == 'B' and len(layer_num) < 2:
        layer_num = '0' + layer_num
    return layer_prefix + layer_num

def main(argv):
    if len(argv) != 3:
        print "Usage: {0} <srcdir> <destdir>".format(argv[0])
        sys.exit(1)
    src  = argv[1] # TODO: check src exists and is zip?
    dest = argv[2] # TODO: check dest exists

    # fail if destination exists but is not a directory
    if os.path.exists(os.path.abspath(dest)) and not os.path.isdir(os.path.abspath(dest)):
        print "Path {} exists and is not a directory.".format(os.path.abspath(dest))
        sys.exit(os.EX_IOERR)

    # try to create destination if it doesn't exist
    if not os.path.isdir(os.path.abspath(dest)):
        try:
            os.makedirs(os.path.abspath(dest))
        except Exception as e:
            print "Failed to create directory at {}.".format(os.path.abspath(dest))
            sys.exit(os.EX_IOERR)

    for gcm, rcp, year, res, files in potential_converts(src):
        for f in files:
            layer = f[-8:-6]
            destname = '{gcm}_{rcp}_{year}_{res}_{layer}'.format(
                gcm = GCM_MAP[gcm],
                rcp = RCP_MAP[rcp],
                year = YEAR_MAP[year],
                res = res,
                layer = LAYER_TYPE_MAP[layer],
            )
            destfile = os.path.join(dest, destname + '.zip')
            with zipfile.ZipFile(destfile, 'w', allowZip64=True) as zip:
                add_source_files(zip, destname, [f,])
                metadata = create_metadata_json(destname, gcm, rcp, year, res,
                    (os.path.basename(f.filename) for f in zip.filelist)
                )
                add_metadata(zip, destname, metadata)

if __name__ == "__main__":
    main(sys.argv)
