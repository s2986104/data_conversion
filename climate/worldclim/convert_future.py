import re
import sys
import glob
import json
import os.path
import zipfile
import calendar
import itertools

JSON_TEMPLATE = 'worldclim.template.json'
TITLE_TEMPLATE = u'WorldClim Future Projection using {gcm} {rcp} at {res} ({year})'

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

YEAR_MAP = {
    '50': '2050',
    '70': '2070',
}

##

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

    
def add_source_files(destzip, destname, filenames):
    for filename in filenames:
        try:
            srczip = zipfile.ZipFile(filename, 'r')
        except zipfile.BadZipfile:
            print "Unable to handle '{}' as zip".format(filename)
            continue
        for itemname in (item.filename for item in srczip.filelist):
            destzip.writestr(
                os.path.join(destname, 'data', itemname), srczip.read(itemname)
            )


def add_metadata(destzip, destname, metadata):
    destzip.writestr(
        os.path.join(destname, 'bccvl', 'metadata.json'), metadata
    )
    
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
    
    for gcm, rcp, year, res, files in potential_converts(src):
        destname = '{gcm}_{rcp}_{year}_{res}'.format(
            gcm = GCM_MAP[gcm], 
            rcp = RCP_MAP[rcp],
            year = YEAR_MAP[year],
            res = res,
        )
        destfile = os.path.join(dest, destname + '.zip')
        with zipfile.ZipFile(destfile, 'w', allowZip64=True) as zip:
            add_source_files(zip, destname, files)
            metadata = create_metadata_json(destname, gcm, rcp, year, res, 
                (os.path.basename(f.filename) for f in zip.filelist)
            )
            add_metadata(zip, destname, metadata)

if __name__ == "__main__":
    main(sys.argv)
