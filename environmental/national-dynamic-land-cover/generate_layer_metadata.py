#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS, collection_by_id
from data_conversion.utils import RegExp

class NDLCLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'national-dynamic-land-cover'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'national-dynamic-land-cover_layers'
    )

    DATASETS = [
        # only one dataset in nsg
        {
            'title': 'Australia, Dynamic Land Cover (2000-2008), {resolution}',
            'categories': ['environmental', 'landcover'],
            'domain': 'terrestrial',
            'acknowledgement': (
                'Lymburner L., Tan P., Mueller N., Thackway R., Lewis A., Thankappan M., Randall L., '
                'Islam A., and Senarath U., (2010), 250 metre Dynamic Land Cover Dataset (1st Edition), '
                'Geoscience Australia, Canberra.'
            ),
            'year': 2004,
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': 'https://data.gov.au/dataset/ds-ga-a05f7893-0031-7506-e044-00144fdd4fa6',
            'partof': [collection_by_id('national-dynamic-land-cover_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE',
                'url': RegExp('^.*dlcdv1-class.*\.tif$')
            },
            'aggs': [],
        },
        {
            'title': 'Australia, Enhanced Vegetation Index (2000-2008), {resolution}',
            'categories': ['environmental', 'landcover'],
            'domain': 'terrestrial',
            'acknowledgement': (
                'Lymburner L., Tan P., Mueller N., Thackway R., Lewis A., Thankappan M., Randall L., '
                'Islam A., and Senarath U., (2010), 250 metre Dynamic Land Cover Dataset (1st Edition), '
                'Geoscience Australia, Canberra.'
            ),
            'year': 2004,
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': 'https://data.gov.au/dataset/ds-ga-a05f7893-0031-7506-e044-00144fdd4fa6',
            'partof': [collection_by_id('national-dynamic-land-cover_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE',
                'url': RegExp('^.*trend-evi.*\.tif$')
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
        if os.path.basename(tiffile) in ('ndlc-2004-250m_dlcdv1-class.tif', 'ndlc-2004-250m_dlcdv1-class-reduced.tif'):
            return {
                'id': 'ISO_CLASS',
                'label': 'CLASSLABEL',
                'value': 'VALUE'
            }
        return None

def main():
    gen = NDLCLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
