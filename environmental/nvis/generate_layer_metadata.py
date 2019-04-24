#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS


class NSGLayerMetadata(BaseLayerMetadata):

    CATEGORY = 'vegetation'
    DATASET_ID = 'nvis'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'nvis'
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
            'coluuid': '33876f6c-b196-4370-9950-ab2b8e6e328e',
            'filter': {
                'genre': 'DataGenreE',
            },
            'aggs': [],
        }
    ]

    COLLECTION = [
        {
            "_type": "Collection",
            "uuid": "33876f6c-b196-4370-9950-ab2b8e6e328e",
            "title": "Australia National Vegetation Information System (NVIS)",
            "description": "Variety and distribution of Australia's native vegetation.\n\nGeographic extent: Australia\nVersion: 4.2\nResolution: {resolution}\nData layers: Australian Major Vegetation Groups, pre-1750 and present".format(resolution=RESOLUTIONS['9']['long']),
            "rights": "CC-BY Attribution 4.0",
            "landingPage": "See <a href=\"http://www.environment.gov.au/land/native-vegetation/national-vegetation-information-system\">http://www.environment.gov.au/land/native-vegetation/national-vegetation-information-system</a>",
            "attribution": ["National Vegetation Information System V4.2 (C) Australian Government Department of the Environment and Energy 2016"],
            "subjects": ["Current datasets"],
            "categories": ["environmental"],
            "BCCDataGenre": ["DataGenreE"],
            "datasets": [],
        }
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['9']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'category': self.CATEGORY,
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
