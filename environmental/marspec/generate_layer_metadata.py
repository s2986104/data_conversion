#!/usr/bin/env python
import os.path
import re

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS, collection_by_id


class MarspecLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'marspec'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'marspec_layers'
    )

    DATASETS = [
        # only one dataset in nsg
        {
            'title': 'Global marine data, Bathymetry (1955-2010), {resolution}'.format(resolution=RESOLUTIONS['300']['long']),
            'categories': ['environmental', 'topography'],
            'domain': 'marine',
            'acknowledgement': (
                'Sbrocco EJ, Barber PH (2013) MARSPEC: Ocean climate layers for marine spatial ecology. '
                'Ecology 94:979. http://dx.doi.org/10.1890/12-1358.1'
            ),
            'year': 2002,
            'license': (
                'Creative Commons Attribution 4.0 '
                'http://creativecommons.org/licenses/by/4.0'
            ),
            'external_url': 'http://marspec.weebly.com/modern-data.html',
            'partof': [collection_by_id('marspec_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE'
            },
            'aggs': [],
        }
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['300']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        # find year range in coverages and use as year_range
        ds_md = {
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['300']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'year': dsdef['year'],
            'year_range': coverages[0]['bccvl:metadata']['year_range'],
            'title': dsdef.get('title').format(**dsdef['filter']),
        }
        return ds_md

    def get_genre(self, md):
        return 'DataGenreE'


def main():
    gen = MarspecLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
