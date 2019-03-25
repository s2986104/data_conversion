#!/usr/bin/env python
import argparse
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

# TODO: need to add resolution to data.json metadata
#       or just to dataset metadata ... probably more appropriate

# TODO: add mimetype somehwere?

CATEGORY = 'environmental'
RESOLUTION = '3 arcsec'
CURRENT_TITLE = 'Australia, Major Vegetation Groups (2016), {resolution} (~90 m)'.format(resolution=RESOLUTION)
EXTERNAL_URL = (
    'http://www.environment.gov.au/land/native-vegetation/national-vegetation-information-system/data-products'
)
LICENSE = (
    'Creative Commons Attribution 3.0 AU'
    'http://creativecommons.org/licenses/by/3.0/AU'
)
ACKNOWLEDGEMENT = ', \n'.join((
        "Environment and Planning Directorate, ACT",
        "Office of Environment and Heritage, NSW",
        "Department of Land Resource Management, Northern Territory",
        "Queensland Herbarium, Department of Science, Information Technology and Innovation",
        "Department for Environment, Water and Natural Resources, South Australia",
        "Department of Primary Industries, Parks, Water and Environment, Tasmania",
        "Department of Environment, Land, Water and Planning, Victoria",
        "Department of Agriculture and Food, Western Australia",
        "Geoscience Australia"
))
SWIFT_CONTAINER = (
    'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
    'nvis'
)


COLLECTION = {
    "_type": "Collection",
    "uuid": "1943f7b3a-3bbc-11e9-90ae-acde48001122",
    "title": "Australian Major Vegetation Groups",
    "description": "Type and distribution of Australia's native vegetation\n\nGeographic extent: Australia\nYear range: 2016\nResolution: 3 arcsec (~90 m)\nData layers: Australian Major Vegetation Groups, pre-1750 and present",
    "rights": "CC-BY Attribution 3.0",
    "landingPage": "See <a href=\"http://www.environment.gov.au/land/native-vegetation/national-vegetation-information-system/data-products\">http://www.environment.gov.au/land/native-vegetation/national-vegetation-information-system/data-products</a>",
    "attribution": ["This raster product is modelled from data originally supplied by the states and territories. The department acknowledges the following departments/organisations: Environment and Planning Directorate, ACT, Office of Environment and Heritage, NSW, Department of Land Resource Management, Northern Territory, Queensland Herbarium, Department of Science, Information Technology and Innovation, Department for Environment, Water and Natural Resources, South Australia, Department of Primary Industries, Parks, Water and Environment, Tasmania, Department of Environment, Land, Water and Planning, Victoria, Department of Agriculture and Food, Western Australia, Geoscience Australia"],
    "subjects": ["Current datasets"],
    "categories": ["environmental"],
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

    "BCCDataGenre": ["DataGenreE"]
}

# Raster attribute table mappings
RAT_MAPPINGS = {'id': 'ASC_ORD', 'label': 'ASC_ORDER_NAME', 'value': 'VALUE'}


def gen_dataset_metadata(genre, coverages):
    ds_md = {
        'category': CATEGORY,
        'genre': genre,
        'resolution': RESOLUTION,
        'acknowledgement': ACKNOWLEDGEMENT,
        'external_url': EXTERNAL_URL,
        'license': LICENSE,
    }
    # collect some bits of metadata from data
    if genre == 'DataGenreFC':
        ds_md['title'] = FUTURE_TITLE
    else:
        ds_md['title'] = CURRENT_TITLE
        # all coverages have the same year and year_range
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
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
                coverage = gen_tif_coverage(tiffile, md['url'], ratmap=RAT_MAPPINGS)
                md['extent_wgs84'] = get_coverage_extent(coverage)
                md['resolution'] = RESOLUTION
                if md['genre'] == 'DataGenreCC':
                    md['acknowledgement'] = ACKNOWLEDGEMENT
                coverage['bccvl:metadata'] = md
                coverage['bccvl:metadata']['uuid'] = gen_coverage_uuid(coverage, 'nvis')
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
    for genre in ("DataGenreCC", "DataGenreFC"):
        # filter coverages by genre and build coverage aggregation over all
        # remaining coverages
        subset = list(filter(
            lambda x: x['bccvl:metadata']['genre'] == genre,
            coverages)
        )
        if not subset:
            continue
        aggs = [] if genre == 'DataGenreCC' else ['emsc', 'gcm', 'year']
        coverage = gen_dataset_coverage(subset, aggs)
        md = gen_dataset_metadata(genre, subset)
        md['extent_wgs84'] = get_coverage_extent(coverage)
        coverage['bccvl:metadata'] = md
        coverage['bccvl:metadata']['uuid'] = gen_coverage_uuid(coverage, 'nvis')
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
