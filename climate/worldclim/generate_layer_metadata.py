#!/usr/bin/env python
import argparse
import os
import os.path
import glob
import json
import sys
import tqdm
import re
import copy

from data_conversion.coverage import (
    gen_tif_metadata,
    gen_tif_coverage,
    get_coverage_extent,
    gen_coverage_uuid,
    gen_dataset_coverage,
)


# TODO: for most metadata we probably would not need to look into tiff file.
#       almost all additional metadata we use in bccvl could be inferred
#       by filename plus hardcoded vocabularies

CATEGORY = 'climate'
AKCNOWLEDGEMENT = (
    "Hijmans, R.J., S.E. Cameron, J.L. Parra, P.G. Jones and A. Jarvis, 2005. "
    "Very high resolution interpolated climate surfaces for global land "
    "areas. International Journal of Climatology 25: 1965-1978."
)
EXTERNAL_URL = "http://worldclim.org/"
LICENSE = "Not Specified"

# TODO: yearly vs. monthly
DATASETS = [
    # current
    {
        # bio + alt
        'title': 'WorldClim 1.4 Current Conditions (~1960-1990), {resolution}',
        'filter': {
            'genre': 'DataGenreCC',
            'month': None,
        },
        'aggs': []
    },
    {
        # monthly tmin, tmax, tmean, prec
        'title': 'WorldClim 1.4 Current Conditions monthly (~1960-1990), {resolution}',
        'filter': {
            'genre': 'DataGenreCC',
            'month': '*',
        },
        'aggs': ['month']
    },
    {
        # bio
        'title': 'WorldClim 1.4 Climate Projection for {gcm}, {resolution}',
        'filter': {
            'month': None,
            'gcm': None,
            'genre': 'DataGenreFC',
        },
        'aggs': ['emsc', 'year'],
    },
    {
        # monthly tmin, tmax, tmean, prec
        'title': 'WorldClim 1.4 Climate Projection monthly for {gcm}, {resolution}',
        'filter': {
            'month': '*',
            'gcm': None,
            'genre': 'DataGenreFC',
        },
        'aggs': ['emsc', 'month', 'year'],
    },
]


SWIFT_CONTAINER = 'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/worldclim_layers'


RESOLUTIONS = {  # udunits arc_minute / arcmin, UCUM/UOM: min_arc
    '30s': '30 arcsec',
    '2-5m': '2.5 arcmin',
    '5m': '5 arcmin',
    '10m': '10 arcmin'
}


def gen_dataset_metadata(dsdef, coverages, resolution):
    genre = dsdef['filter']['genre']
    ds_md = {
        'category': CATEGORY,  # TODO: need elevation category as well for current
        'genre': genre,
        'resolution': RESOLUTIONS[resolution],
        'acknowledgement': AKCNOWLEDGEMENT,
        'external_url': EXTERNAL_URL,
        'license': LICENSE,
        'title': dsdef['title'].format(
            resolution=RESOLUTIONS[resolution],
            gcm=dsdef['filter'].get('gcm'),
        )
    }
    if genre == 'DataGenreCC':
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
    return ds_md


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


def main(argv):
    opts = parse_args()
    opts.srcdir = os.path.abspath(opts.srcdir)

    datajson = os.path.join(opts.srcdir, 'data.json')
    print("Generate data.json")
    if not os.path.exists(datajson) or opts.force:
        print("Rebuild data.json")
        coverages = []
        tiffiles = sorted(glob.glob(os.path.join(opts.srcdir, '**/*.tif'),
                                    recursive=True))
        for tiffile in tqdm.tqdm(tiffiles):
            try:
                md = gen_tif_metadata(tiffile, opts.srcdir, SWIFT_CONTAINER)
                coverage = gen_tif_coverage(tiffile, md['url'])
                md['extent_wgs84'] = get_coverage_extent(coverage)
                coverage['bccvl:metadata'] = md
                coverage['bccvl:metadata']['uuid'] = gen_coverage_uuid(coverage, 'worldclim')
                coverages.append(coverage)
            except Exception as e:
                print('Failed to generate metadata for:', tiffile, e)

        print("Write data.json")
        with open(datajson, 'w') as mdfile:
            json.dump(coverages, mdfile, indent=2)
    else:
        print("Using existing data.json")
        coverages = json.load(open(datajson))

    print("Generate datasets.json")
    datasets = []
    # collect all emission scenarios from coverages
    GCMS = sorted({cov['bccvl:metadata']['gcm'] for cov in coverages if 'gcm' in cov['bccvl:metadata']})
    for dsdef in DATASETS:
        # make a copy so that we can modify it
        dsdef = copy.deepcopy(dsdef)
        for resolution in RESOLUTIONS:
            cov_filter = dsdef['filter']
            # add resolution filter:
            cov_filter['url'] = re.compile(r'https://.*/.*{}.*\.tif'.format(resolution))
            if 'gcm' not in cov_filter:
                # current
                subset = list(filter(
                    lambda x: match_coverage(x, cov_filter),
                    coverages
                ))
                if not subset:
                    print("No Data matched for {}".format(cov_filter))
                    continue
                coverage = gen_dataset_coverage(subset, dsdef['aggs'])
                md = gen_dataset_metadata(dsdef, subset, resolution)
                md['extent_wgs84'] = get_coverage_extent(coverage)
                coverage['bccvl:metadata'] = md
                coverage['bccvl:metadata']['uuid'] = gen_coverage_uuid(coverage, 'worldclim-1.4')
                datasets.append(coverage)
            else:
                # future
                for gcm in GCMS:
                    cov_filter['gcm'] = gcm
                    subset = list(filter(
                        lambda x: match_coverage(x, cov_filter),
                        coverages
                    ))
                    if not subset:
                        print("No Data matched for {}".format(cov_filter))
                        continue
                    coverage = gen_dataset_coverage(subset, dsdef['aggs'])
                    md = gen_dataset_metadata(dsdef, subset, resolution)
                    md['extent_wgs84'] = get_coverage_extent(coverage)
                    md['gcm'] = cov_filter['gcm']
                    coverage['bccvl:metadata'] = md
                    coverage['bccvl:metadata']['uuid'] = gen_coverage_uuid(coverage, 'worldclim-1.4')
                    datasets.append(coverage)


    print("Write datasets.json")
    # save all the data
    with open(os.path.join(opts.srcdir, 'datasets.json'), 'w') as mdfile:
        json.dump(datasets, mdfile, indent=2)



    # if len(argv) != 2:
    #     print "Usage: {0} <srcdir>".format(argv[0])
    #     sys.exit(1)
    # srcdir = argv[1]

    # resolution = '5 arcmin'
    # resol = [key for key in RESOLUTION.keys() if RESOLUTION[key] == resolution]
    # print "Resolution = ", resol
    # category = ["climate", "topography"]
    # for subdir in ('current-layers', 'future-layers'):
    #     for dataset in glob.glob(os.path.join(srcdir, subdir, '*_' + resol[0] + '_*')):
    #         print "Processing ", dataset
    #         gen_metadatajson(JSON_TEMPLATE, dataset, SWIFT_CONTAINER)

    # for genre in ("DataGenreCC", "DataGenreFC"):
    #     dsmd = gen_dataset_metadata(JSON_TEMPLATE, category, genre, resolution)
    #     if dsmd:
    #         datasetmds.append(dsmd)

    # # save layer metadata to file
    # prefix = 'worldclim_' + resolution.replace(' ', '')
    # with open(os.path.join(srcdir, prefix + '_layer_metadata.json'), 'w') as mdfile:
    #     json.dump({"type": "layer", "data": layermds}, mdfile, indent=4)

    # # save dataset metadata to file
    # with open(os.path.join(srcdir, prefix + '_dataset_metadata.json'), 'w') as dsmdfile:
    #     json.dump({"type": "dataset", "data": datasetmds}, dsmdfile, indent=4)

    # # save collection


if __name__ == "__main__":
    main(sys.argv)
