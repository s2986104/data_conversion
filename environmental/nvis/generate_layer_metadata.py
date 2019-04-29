#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS, collection_by_id


class NSGLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'nvis'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'nvis_layers'
    )

    DATASETS = [
        # only one dataset in nsg
        {
            'title': 'Australia, Major Vegetation Groups (NVIS) (v4.2), {resolution}',
            'acknowledgement': (
                'National Vegetation Information System V4.2 (C) Australian Government Department of the '
                'Environment and Energy 2016'
            ),
            #'year': 2012,
            'license': (
                'Creative Commons Attribution 4.0 '
                'http://creativecommons.org/licenses/by/4.0'
            ),
            'external_url': 'http://www.environment.gov.au/land/native-vegetation/national-vegetation-information-system',
            'partof': [collection_by_id('nvis_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE',
            },
            'aggs': [],
        }
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['9']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'categories': ['environmental', 'vegetation'],
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['9']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            #'year': dsdef.get('year'),
            'title': dsdef.get('title').format(
                resolution=RESOLUTIONS['9']['long'],
                **dsdef['filter']
            ),
        }
        # find min/max years in coverages and use as year_range
        #years = [cov['bccvl:metadata']['year'] for cov in coverages]
        #ds_md['year_range'] = [min(years), max(years)]
        ds_md['version'] = '4.2'
        return ds_md

    def get_genre(self, md):
        return 'DataGenreE'

    def get_rat_map(self, tiffile):
        return {
            'id': 'MVG_NAME',
            'label': 'MVG_COMMON_DESC',
            'value': 'VALUE'
        }

def main():
    gen = NSGLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
