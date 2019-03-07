#!/usr/bin/env python
import argparse
import copy
import glob
import itertools
import json
import os
import os.path
import re

import tqdm

from data_conversion.coverage import (
    gen_tif_metadata,
    gen_tif_coverage,
    get_coverage_extent,
    gen_coverage_uuid,
    gen_dataset_coverage,
)

# TODO: need to add resolution to data.json metadata
#       or just to dataset metadata ... probably more appropriate

# TODO: add mimetype somehwere?

DATASETNAME = 'climond'
CATEGORY = 'climate'
RESOLUTION = '10 arcmin'
SWIFT_CONTAINER = (
    'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
    'climond'
)

RESOLUTIONS = {  # udunits arc_minute / arcmin, UCUM/UOM: min_arc
    '10 arcmin': '10 arcmin (~20km)',
}

ACKNOWLEDGEMENT = (
            'Kriticos, D.J., B.L. Webber, A. Leriche, N. Ota, I. Macadam, J. Bathols '
            '& J.K. Scott.  2012.  CliMond: global high-resolution historical and '
            'future scenario climate surfaces for bioclimatic modelling.  Methods '
            'in Ecology & Evolution, 3(1), 53 - 64.'
)

DATASETS = [
    # current
    {
        # bio
        'title': 'CliMond (global), current climate {year}, {resolution}',
        'acknowledgement': ACKNOWLEDGEMENT,
        'external_url': 'https://www.climond.org/ClimateData.aspx',
        'license': (
            'Creative Commons Attribution 3.0 AU'
            'https://creativecommons.org/licenses/by/3.0/au'
        ),
        'filter': {
            'genre': 'DataGenreCC'
        },
        'aggs': [], 
    },
    {
        # bio
        'title': 'CliMond (global), Climate Projection, {emsc} based on {gcm}, {resolution} - {year}',
        'acknowledgement': ACKNOWLEDGEMENT,
        'external_url': 'https://www.climond.org/ClimateData.aspx',
        'license': (
            'Creative Commons Attribution 3.0 AU'
            'https://creativecommons.org/licenses/by/3.0/au'
        ),
        'filter': {
            'genre': 'DataGenreFC',
            'gcm': None,
            'emsc': None,
            'year': None
        },
        'aggs': [], 
    }    
]

COLLECTION = {
    "_type": "Collection",
    "uuid": "9a865673-a8f1-4e05-9f4d-8b950b8206b9",
    "title": "CliMond climate data",
    "description": "Global current and future climate data\n\nGeographic extent: Global\nYear range: 1961-1990, 2030, 2050, 2070, 2090, 2100\nResolution: 10 arcmin (~20 km)\nData layers: B01-35",
    "rights": "CC-BY Attribution 3.0 AU",
    "landingPage": "See <a href=\"https://www.climond.org/ClimateData.aspx\">CliMond Climate Data</a>",
    "attribution": [ACKNOWLEDGEMENT],
    "subjects": ["Current datasets", "Future datasets"],
    "categories": ["climate"],
    "datasets": [
        # {
        #     "uuid": "8f6e3ea5-1caf-5562-a580-aa23bbe7c975",
        #     "title": "Australia, Current Climate (1976-2005), 2.5 arcmin (~5 km)"
        # },
        # {
        #     "uuid": "fecc0b23-4199-5b49-ac6c-45c3c1249f3e",
        #     "title": "Australia, Climate Projection, 2.5 arcmin"
        # }
    ],

    "BCCDataGenre": ["DataGenreCC", "DataGenreFC"]
}


def gen_dataset_metadata(dsdef, coverages, **kw):
    ds_md = {
        'category': CATEGORY,
        'genre': kw['genre'],
        'resolution': RESOLUTION,
        'acknowledgement': dsdef.get('acknowledgment'),
        'external_url': dsdef.get('external_url'),
        'license': dsdef.get('license'),
        # TODO: format title
        'title': dsdef['title'].format(resolution=RESOLUTIONS[RESOLUTION], **kw)
    }
    # collect some bits of metadata from data
    if dsdef['filter']['genre'] == 'DataGenreCC':
        # all coverages have the same year and year_range
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
    return ds_md


# TODO: duplicate in worldclim/generate_metadata_layers
def match_coverage(cov, attrs):
    # used to filter set of coverages
    md = cov['bccvl:metadata']
    for attr, value in attrs.items():
        if isinstance(value, re.Pattern):
            if not value.match(md[attr]):
                return False
            continue
        if value is None:
            # attr should not be there
            if attr in md:
                return False
            continue
        if value == '*':
            # attr should be there
            if attr not in md:
                return False
            continue
        if md.get(attr) != attrs[attr]:
            return False
    return True


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true',
                        help='Re generate data.json form tif files.')
    parser.add_argument('srcdir')
    return parser.parse_args()


def main():
    # TODO: we need a mode to just update as existing json file without parsing
    #       all tiff files. This would be useful to just update titles and
    #       things.
    #       could probably also be done in a separate one off script?
    opts = parse_args()
    opts.srcdir = os.path.abspath(opts.srcdir)

    datajson = os.path.join(opts.srcdir, 'data.json')
    print("Generate data.json")
    if not os.path.exists(datajson) or opts.force:
        print("Rebuild data.json")
        # rebuild data.json
        coverages = []
        # generate all coverages inside source folder
        tiffiles = sorted(glob.glob(os.path.join(opts.srcdir, '**/*.tif'),
                                    recursive=True))
        for tiffile in tqdm.tqdm(tiffiles):
            try:
                md = gen_tif_metadata(tiffile, opts.srcdir, SWIFT_CONTAINER)
                coverage = gen_tif_coverage(tiffile, md['url'])
                md['extent_wgs84'] = get_coverage_extent(coverage)
                md['resolution'] = RESOLUTION
                md['acknowledgement'] = ACKNOWLEDGEMENT
                coverage['bccvl:metadata'] = md
                coverage['bccvl:metadata']['uuid'] = gen_coverage_uuid(coverage, DATASETNAME)
                coverages.append(coverage)
            except Exception as e:
                print('Failed to generate metadata for:', tiffile, e)
                raise

        print("Write data.json")
        with open(datajson, 'w') as mdfile:
            json.dump(coverages, mdfile, indent=2)
    else:
        print("Use existing data.json")
        coverages = json.load(open(datajson))

    print("Generate datasets.json")
    datasets = []
    # collect all emscs, gcms, and years from coverages
    GCMS = sorted({cov['bccvl:metadata']['gcm'] for cov in coverages if 'gcm' in cov['bccvl:metadata']})
    EMSCS = sorted({cov['bccvl:metadata']['emsc'] for cov in coverages if 'emsc' in cov['bccvl:metadata']})
    YEARS = sorted({cov['bccvl:metadata']['year'] for cov in coverages if 'emsc' in cov['bccvl:metadata']})
    # generate datasets for db import
    for dsdef in DATASETS:
        # make a copy so that we can modify the filters
        dsdef = copy.deepcopy(dsdef)
        cov_filter = dsdef['filter']
        if cov_filter['genre'] == 'DataGenreCC':
            # current
            subset = list(filter(
                lambda x: match_coverage(x, cov_filter),
                coverages
            ))
            if not subset:
                print("No Data matched for {}".format(cov_filter))
                continue
            coverage = gen_dataset_coverage(subset, dsdef['aggs'])
            md = gen_dataset_metadata(dsdef, subset, genre=cov_filter['genre'])
            md['extent_wgs84'] = get_coverage_extent(coverage)
            coverage['bccvl:metadata'] = md
            coverage['bccvl:metadata']['uuid'] = gen_coverage_uuid(coverage, DATASETNAME)
            datasets.append(coverage)
        else:
            # future
            for gcm, emsc, year in itertools.product(GCMS, EMSCS, YEARS):
                cov_filter.update({
                    'gcm': gcm,
                    'emsc': emsc,
                    'year': year
                })
                subset = list(filter(
                    lambda x: match_coverage(x, cov_filter),
                    coverages
                ))
                if not subset:
                    print("No Data matched for {}".format(cov_filter))
                    continue
                coverage = gen_dataset_coverage(subset, dsdef['aggs'])
                md = gen_dataset_metadata(dsdef, subset, gcm=gcm, emsc=emsc, year=year, genre=cov_filter['genre'])
                md['extent_wgs84'] = get_coverage_extent(coverage)
                coverage['bccvl:metadata'] = md
                coverage['bccvl:metadata']['uuid'] = gen_coverage_uuid(coverage, DATASETNAME)
                datasets.append(coverage)

    print("Write datasets.json")
    # save all the data
    with open(os.path.join(opts.srcdir, 'datasets.json'), 'w') as mdfile:
        json.dump(datasets, mdfile, indent=2)

    print("Write collection.json")
    with open(os.path.join(opts.srcdir, 'collection.json'), 'w') as mdfile:
        # add datasets
        for ds in datasets:
            COLLECTION['datasets'].append({
                "uuid": ds['bccvl:metadata']['uuid'],
                "title": ds['bccvl:metadata']['title']
            })
        json.dump([COLLECTION], mdfile, indent=2)


if __name__ == "__main__":
    main()
