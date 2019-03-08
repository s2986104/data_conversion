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

DATASETNAME = 'cruclim'
CATEGORY = 'climate'
RESOLUTION = '30 arcmin'
SWIFT_CONTAINER = (
    'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
    'cruclim'
)

RESOLUTIONS = {  # udunits arc_minute / arcmin, UCUM/UOM: min_arc
    '30 arcmin': '30 arcmin (~50km)',
}

ACKNOWLEDGEMENT = (
            'University of East Anglia Climatic Research Unit; Harris, I.C.; '
            'Jones, P.D. (2015): CRU TS3.23: Climatic Research Unit (CRU) '
            'Time-Series (TS) Version 3.23 of High Resolution Gridded Data '
            'of Month-by-month Variation in Climate (Jan. 1901- Dec. 2014). '
            'Centre for Environmental Data Analysis, 09 November 2015. '
            'doi:10.5285/4c7fdfa6-f176-4c58-acee-683d5e9d2ed5.'
)

DATASETS = [
    # current
    {
        # bio
        'title': 'CRUclim (global), current climate (1976-2005), {resolution}',
        'acknowledgement': ACKNOWLEDGEMENT,
        'external_url': 'http://www.ceda.ac.uk/',
        'license': (
            'Open Government Licence for Public Sector Information (UK) '
            'http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/'
        ),
        'filter': {
            'genre': 'DataGenreCC'
        },
        'aggs': [], 
    }
]

COLLECTION = {
    "_type": "Collection",
    "uuid": "06d2de1c-559a-4e91-80ea-24aec53eca3f",
    "title": "Alimatic Research Unit (CRU) climate data",
    "description": "Global climate data from monthly observations collated by the University of East Anglia Climatic Research Unit",
    "rights": "Open Government Licence for Public Sector Information (UK)",
    "landingPage": "http://www.ceda.ac.uk/",
    "attribution": [ACKNOWLEDGEMENT],
    "subjects": ["Current datasets"],
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

    "BCCDataGenre": ["DataGenreCC"]
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
                if md['genre'] == 'DataGenreCC':
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
    # collect all years from coverages
    YEARS = sorted({cov['bccvl:metadata']['year'] for cov in coverages if 'year' in cov['bccvl:metadata']})
    # generate datasets for db import
    for dsdef in DATASETS:
        # make a copy so that we can modify the filters
        dsdef = copy.deepcopy(dsdef)
        cov_filter = dsdef['filter']
        if cov_filter['genre'] == 'DataGenreCC':
            # current
            for year in YEARS:
                cov_filter.update({
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
                md = gen_dataset_metadata(dsdef, subset, genre=cov_filter['genre'])
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
