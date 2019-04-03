#!/usr/bin/env python
import os.path
impore re

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS


class NDLCLayerMetadata(BaseLayerMetadata):

    CATEGORY = 'landcover'  # scientific type
    DATASET_ID = 'national-dynamic-land-cover'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'national-dynamic-land-cover'
    )

    DATASETS = [
        # only one dataset in nsg
        {
            'title': 'Australia, Dynamic Land Cover (2000-2008), {resolution}',
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
            'filter': {
                'genre': 'DataGenreE',
                'url': re.compile('^.*dlcdv1-class.*\.tif$')
            },
            'aggs': [],
        },
        {
            'title': 'Australia, Enhanced Vegetation Index (2000-2008), {resolution}',
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
            'filter': {
                'genre': 'DataGenreE',
                'url': re.compile('^.*trend-evi.*\.tif$')
            },
            'aggs': [],
        }
    ]

    COLLECTION = {
        "_type": "Collection",
        "uuid": "b735a408-473e-4080-be4c-9ee3628417dc",
        "title": "Australia Land Cover",
        "description": "Comprehensive land cover data for Australia.\n\nGeographic extent: Australia\nYear range: 2000-2008\nResolution: {resolution}\nData layers: Dynamic Land Cover, Enhanced Vegetation Index (min, max, mean)".format(resolution=RESOLUTIONS['9']['long']),
        "rights": "CC-BY Attribution 3.0",
        "landingPage": "See <a href=\"http://www.ga.gov.au/scientific-topics/earth-obs/accessing-satellite-imagery/landcover/executive-summary\">http://www.ga.gov.au/scientific-topics/earth-obs/accessing-satellite-imagery/landcover/executive-summary</a>",
        "attribution": ["Lymburner L, Tan P, Mueller N, Thackway R, Lewis A, Thankappan M, Randall L, Islam A, Senarath U (2011) The National Dynamic Land Cover Dataset (v1.0), Geoscience Australia, Canberra."],
        "subjects": ["Current datasets"],
        "categories": ["environmental"],
        "BCCDataGenre": ["DataGenreE"],
        "datasets": [],
    }

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['9']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'category': self.CATEGORY,                  # scientific type
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
        # find min/max years in coverages and use as year_range
        years = [cov['bccvl:metadata']['year'] for cov in coverages]
        ds_md['year_range'] = [min(years), max(years)]
        return ds_md

    def get_genre(self, md):
        return 'DataGenreE'

    def get_rat_map(self, tiffile):
        if os.path.basename(tiffile) in ('ndlc-2004-250m_DLCDv1_Class.tif', 'ndlc-2004-250m_DLCDv1_Class_Reduced.tif'):
            return {
                'id': 'ISO_CLASS',
                'label': 'CLASSLABEL',
                'value': 'VALUE'
            }
        return None

def main():
    gen = NSGLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
