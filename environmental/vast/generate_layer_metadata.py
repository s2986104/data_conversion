#!/usr/bin/env python
import os.path
import re

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS


class VASTLayerMetadata(BaseLayerMetadata):

    CATEGORY = 'vegetation'  # scientific type
    DATASET_ID = 'vast'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'vast'
    )

    DATASETS = [
        # only one dataset in vast
        {
            'title': 'Australia, Vegetation Assets, States and Transitions (VAST) (1995-2006), {resolution}',
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
            'filter': {
                'genre': 'DataGenreE'
            },
            'aggs': [],
        }
    ]

    COLLECTION = {
        "_type": "Collection",
        "uuid": "ef493e7d-1b06-403b-844a-ca771622085a",
        "title": "Australia Vegetation Assets, States and Transitions (VAST)",
        "description": "Classification for Australian vegetation according to its degree of anthropogenic modification from a natural state.\n\nGeographic extent: Australia\nYear range: 2008\nResolution: {resolution}\nData layers: VAST classification".format(resolution=RESOLUTIONS['30']['long']),
        "rights": "CC-BY Attribution 3.0",
        "landingPage": "See <a href=\"http://data.daff.gov.au/anrdl/metadata_files/pa_vast_g9abll0032008_11a.xml\">http://data.daff.gov.au/anrdl/metadata_files/pa_vast_g9abll0032008_11a.xml</a>",
        "attribution": ["Lesslie R, Thackway R, Smith J (2010) A national-level Vegetation Assets, States and Transitions (VAST) dataset for Australia (version 2), Bureau of Rural Sciences, Canberra."],
        "subjects": ["Current datasets"],
        "categories": ["environmental"],
        "BCCDataGenre": ["DataGenreE"],
        "datasets": [],
    }

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['30']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'category': self.CATEGORY,                  # scientific type
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
