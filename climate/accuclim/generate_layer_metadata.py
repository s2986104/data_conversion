#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS


class AccuClimLayerMetadata(BaseLayerMetadata):

    CATEGORY = 'climate'
    DATASET_ID = 'accuclim'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'accuclim_layers'
    )

    DATASETS = [
        # current
        {
            # bio
            'title': 'accuCLIM (Wet Tropics Australia), 30-year average either side of ({year}), {resolution}',
            'acknowledgement': (
                'Storlie, C.J., Phillips, B.L., VanDerWal, J.J., and Williams, S.E. (2013) '
                'Improved spatial estimates of climate predict patchier species distributions. '
                'Diversity and Distributions, 19 (9). pp. 1106-1113.'
            ),
            'external_url': 'https://researchdata.ands.org.au/accuclim-30-year-heritage-area/654267',
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'filter': {
                'genre': 'DataGenreCC',
                'year': None
            },
            'aggs': [], 
        }
    ]

    COLLECTION = {
        "_type": "Collection",
        "uuid": "1db9e574-2f14-11e9-b0ea-0242ac110002",
        "title": "ACCUclim climate data",
        "description": "A set of 7 temperature variables for the Wet Tropics area in north-east Australia\n\nGeographic extent: Wet Tropics, Australia\nYear range: 1950-2015\nResolution: {resolution}\nData layers: B01-07".format(resolution=RESOLUTIONS['9']['long']),
        "rights": "CC-BY Attribution 3.0",
        "landingPage": "https://researchdata.ands.org.au/accuclim-30-year-heritage-area/654267",
        "attribution": ["Storlie, C.J., Phillips, B.L., VanDerWal, J.J., and Williams, S.E. (2013) Improved spatial estimates of climate predict patchier species distributions. Diversity and Distributions, 19 (9). pp. 1106-1113."],
        "subjects": ["Current datasets"],
        "categories": ["climate"],
        "BCCDataGenre": ["DataGenreCC"],
        "datasets": [],
    }

    def parse_filename(self, tiffile):
        return {
            # all 9 arcsec
            'resolution': RESOLUTIONS['9']['long']
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'category': self.CATEGORY,
            'genre': dsdef['filter']['genre'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'resolution': RESOLUTIONS['9']['long'],
            'title': dsdef['title'].format(
                resolution=RESOLUTIONS['9']['long'],
                **dsdef['filter'])
        }
        # collect some bits of metadata from data
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        return ds_md


def main():
    gen = AccuClimLayerMetadata()
    gen.main()


if __name__ == "__main__":
    main()
