#!/usr/bin/env python
import os.path
import re

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS


class MarspecLayerMetadata(BaseLayerMetadata):

    CATEGORY = 'topography'
    DATASET_ID = 'marspec'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'marspec'
    )

    DATASETS = [
        # only one dataset in nsg
        {
            'title': 'Global marine data, Bathymetry (1955-2010), {resolution}'.format(resolution=RESOLUTIONS['300']['long']),
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
            'coluuid': '2d8021b7-a971-4c9b-b194-37d1ff91a965',
            'filter': {
                'genre': 'DataGenreE'
            },
            'aggs': [],
        }
    ]


    COLLECTION = [
        {
            "_type": "Collection",
            "uuid": "2d8021b7-a971-4c9b-b194-37d1ff91a965",
            "title": "Global Marine Environmental Data (MARSPEC)",
            "description": "Global ocean bathymetry data.\n\nGeographic extent: Global\nYear range: 1955-2010\nResolution: {resolution}\nData layers: Bathymetry".format(resolution=RESOLUTIONS['300']['long']),
            "rights": "CC-BY Attribution 4.0",
            "landingPage": "See <a http://marspec.weebly.com/modern-data.html\">http://marspec.weebly.com/modern-data.html</a>",
            "attribution": [
                "Sbrocco EJ, Barber PH (2013) MARSPEC: Ocean climate layers for marine spatial ecology. Ecology 94:979. http://dx.doi.org/10.1890/12-1358.1"
            ],
            "subjects": ["Current datasets"],
            "categories": ["environmental"],
            "BCCDataGenre": ["DataGenreE"],
            "datasets": [],
        }
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['300']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        # find year range in coverages and use as year_range
        ds_md = {
            'category': self.CATEGORY,
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
