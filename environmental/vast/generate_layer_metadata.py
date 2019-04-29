#!/usr/bin/env python
import os.path
import re

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS, collection_by_id


class VASTLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'vast'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'vast_layers'
    )

    DATASETS = [
        # only one dataset in vast
        {
            'title': 'Australia, Vegetation Assets, States and Transitions (VAST) (1995-2006), {resolution}',
            'categories': ['environmental', 'vegetation'],
            'domain': 'terrestrial',
            'acknowledgement': (
                'All visual and pubished material must acknowledge the Australian Bureau of Agricultural '
                'and Resource Economics and Sciences (ABRES) compiled and derived the dataset.'
            ),
            'year': 2008,
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au. '
                'Content supplied by third-parties is excluded from this license.'
            ),
            'external_url': 'http://data.daff.gov.au/anrdl/metadata_files/pa_vast_g9abll0032008_11a.xml',
            'partof': [collection_by_id('vast_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE'
            },
            'aggs': [],
        }
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['30']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['30']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'year': dsdef.get('year'),
            'title': dsdef.get('title').format(
                resolution=RESOLUTIONS['30']['long'],
                **dsdef['filter']
            ),
        }
        # find year_range from coverages
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        return ds_md

    def get_genre(self, md):
        return 'DataGenreE'

    def get_rat_map(self, tiffile):
        if os.path.basename(tiffile) in ('vast-2008-1km_vastgridv2-1k.tif'):
            return {
                'id': 'VAST_CLASS',
                'label': 'LANDSCAPE_MATRIX',
                'value': 'VALUE'
            }
        return None

def main():
    gen = VASTLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
