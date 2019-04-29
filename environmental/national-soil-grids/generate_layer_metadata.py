#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS, collection_by_id


class NSGLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'national_soil_grids'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'national_soil_grids_layers'
    )

    DATASETS = [
        # only one dataset in nsg
        {
            'title': 'Australia, Soil Grids (2012), {resolution}',
            'categories': ['environmental', 'substrate'],
            'domain': 'terrestrial',
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
            'partof': [collection_by_id('national_soil_grids_layers')['uuid']],
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
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['9']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'year': dsdef.get('year'),
            'title': dsdef.get('title').format(
                resolution=RESOLUTIONS['9']['long'],
                **dsdef['filter']
            ),
        }
        # find year_range from coverages
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        return ds_md

    def get_genre(self, md):
        return 'DataGenreE'

    def get_rat_map(self, tiffile):
        if os.path.basename(tiffile) == 'nsg-2011-250m_asc.tif':
            return {
                'id': 'ASC_ORD',
                'label': 'ASC_ORDER_NAME',
                'value': 'VALUE'
            }
        return None

def main():
    gen = NSGLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
