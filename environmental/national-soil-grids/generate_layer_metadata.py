#!/usr/bin/env python
import argparse
import copy
import glob
import json
import os
import os.path

import tqdm

from data_conversion.coverage import (
    gen_tif_metadata,
    gen_tif_coverage,
    get_coverage_extent,
    gen_coverage_uuid,
    gen_dataset_coverage,
)
from data_conversion.utils import match_coverage

# TODO: add mimetype somehwere?

CATEGORY = 'environmental'
SWIFT_CONTAINER = (
    'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
    'national_soil_grids'
)

DATASETS = [
    # only one dataset in nsg
    {
        'title': 'Australia, National Soil Grids (2012), 9 arsec (~250m)',
        'acknowledgement': (
            'National soil data provided by the Australian Collaborative Land Evaluation Program ACLEP, '
            'endorsed through the National Committee on Soil and Terrain NCST (www.clw.csiro.au/aclep).'
        ),
        'year': 2012,
        'license': (
            'Creative Commons Attribution 3.0 AU '
            'http://creativecommons.org/licenses/by/3.0/au'
        ),
        'external_url': 'http://www.asris.csiro.au/themes/NationalGrids.html',
        'filter': {
            'genre': 'DataGenreE',
        },
        'aggs': [],
    }
]

COLLECTION = {
    "_type": "Collection",
    "uuid": "1e7eb0c57-33f1-11e9-bbab-acde48001122",
    "title": "National Soil Grids Australia",
    "description": "Soil classification and attributes\n\nGeographic extent: Australia\nYear range: 2012\nResolution: 9 arcsec (~250 m)\nData layers: Soil classification, Bulk density, Clay content, Plant available water capacity, pH",
    "rights": "CC-BY Attribution 3.0",
    "landingPage": "See <a href=\"http://www.asris.csiro.au/themes/NationalGrids.html\">http://www.asris.csiro.au/themes/NationalGrids.html</a>",
    "attribution": ["National soil data provided by the Australian Collaborative Land Evaluation Program ACLEP, endorsed through the National Committee on Soil and Terrain NCST (www.clw.csiro.au/aclep)."],
    "subjects": ["Current datasets"],
    "categories": ["environmental"],
    "datasets": [
    ],

    "BCCDataGenre": ["DataGenreE"]
}

# Raster attribute table mappings
RAT_MAPPINGS = {'id': 'ASC_ORD', 'label': 'ASC_ORDER_NAME', 'value': 'VALUE'}


def gen_dataset_metadata(dsdef, coverages, **kw):
    ds_md = {
        'category': CATEGORY,
        'genre': kw['genre'],
        'resolution': '9 arcsec (~250m)',
        'acknowledgement': dsdef.get('acknowledgment'),
        'external_url': dsdef.get('external_url'),
        'license': dsdef.get('license'),
        'title': dsdef.get('title'),
        'year': dsdef.get('year'),
    }
    # find min/max years in coverages and use as year_range
    years = [cov['bccvl:metadata']['year'] for cov in coverages]
    ds_md['year_range'] = [min(years), max(years)]
    return ds_md


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
                md['genre'] = 'DataGenreE'
                coverage = gen_tif_coverage(tiffile, md['url'], ratmap=RAT_MAPPINGS)
                md['extent_wgs84'] = get_coverage_extent(coverage)
                md['resolution'] = '9 arcsec',
                coverage['bccvl:metadata'] = md
                coverage['bccvl:metadata']['uuid'] = gen_coverage_uuid(coverage, 'national_soil_grids')
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
    # generate datasets for db import
    for dsdef in DATASETS:
        # copy filter to avoid modifying original
        dsdef = copy.deepcopy(dsdef)
        cov_filter = dsdef['filter']
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
        coverage['bccvl:metadata']['uuid'] = gen_coverage_uuid(coverage, 'national-soil-grids')
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
