#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS

class ANUClimLayerMetadata(BaseLayerMetadata):

    CATEGORY = 'climate'
    DATASET_ID = 'anuclim'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'anuclim_layers'
    )

    DATASETS = [
        # current
        {
            # bio
            'title': 'ANUClim (Australia), Current Climate {month}, (1976-2005), {resolution}',
            'acknowledgement': (
                'Hutchinson M, Kesteven J, Xu T (2014) Monthly climate data: ANUClimate 1.0, '
                '0.01 degree, Australian Coverage, 1976-2005. Australian National University, '
                'Canberra, Australia. Made available by the Ecosystem Modelling and Scaling '
                'Infrastructure (eMAST, http://www.emast.org.au) of the Terrestrial Ecosystem '
                'Research Network (TERN, http://www.tern.org.au).'
            ),
            'external_url': 'https://researchdata.ands.org.au/anuclimate-collection/983248',
            'license': (
                'Creative Commons Attribution 4.0'
                'https://creativecommons.org/licenses/by/4.0/'
            ),
            'coluuid': 'd75094b7-0eac-4176-accb-d5a23f5d4b4d',
            'filter': {
                'genre': 'DataGenreCC',
                'month': None
            },
            'aggs': [], 
        }
    ]

    COLLECTION = [
        {
            "_type": "Collection",
            "uuid": "d75094b7-0eac-4176-accb-d5a23f5d4b4d",
            "title": "ANUclim climate data",
            "description": "Monthly climate data for the Australian continent\n\nGeographic extent: Australia\nYear range: 1976-2005\nResolution: {resolution}\nData layers: Minimum/Maximum Temperature, Precipitation, Evaporation, Vapour Pressure".format(resolution=RESOLUTIONS['30']['long']),
            "rights": "CC-BY Attribution 4.0",
            "landingPage": "See <a href=\"https://researchdata.ands.org.au/anuclimate-collection/983248/\">https://researchdata.ands.org.au/anuclimate-collection/983248</a>",
            "attribution": [
                (
                    'Hutchinson M, Kesteven J, Xu T (2014) Monthly climate data: ANUClimate 1.0, '
                    '0.01 degree, Australian Coverage, 1976-2005. Australian National University, '
                    'Canberra, Australia. Made available by the Ecosystem Modelling and Scaling '
                    'Infrastructure (eMAST, http://www.emast.org.au) of the Terrestrial Ecosystem '
                    'Research Network (TERN, http://www.tern.org.au).'
                )
            ],
            "subjects": ["Current datasets"],
            "categories": ["climate"],
            "BCCDataGenre": ["DataGenreCC"],
            "datasets": [],
        }
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['30']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'category': self.CATEGORY,
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['30']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': dsdef['title'].format(
                resolution=RESOLUTIONS['30']['long'],
                **dsdef['filter']
            ),
            'month': dsdef['filter']['month']
        }
        # all coverages have the same year and year_range
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        return ds_md


def main():
    gen = ANUClimLayerMetadata()
    gen.main()

    
if __name__ == "__main__":
    main()
