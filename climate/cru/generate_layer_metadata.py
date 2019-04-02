#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS


class CRULayerMetadata(BaseLayerMetadata):

    CATEGORY = 'climate'
    DATASET_ID = 'cruclim'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'cruclim_layers'
    )

    DATASETS = [
        # current
        {
            # bio
            'title': 'CRUclim (global), current climate (1976-2005), {resolution}',
            'acknowledgement': (
                'University of East Anglia Climatic Research Unit; Harris, I.C.; '
                'Jones, P.D. (2015): CRU TS3.23: Climatic Research Unit (CRU) '
                'Time-Series (TS) Version 3.23 of High Resolution Gridded Data '
                'of Month-by-month Variation in Climate (Jan. 1901- Dec. 2014). '
                'Centre for Environmental Data Analysis, 09 November 2015. '
                'doi:10.5285/4c7fdfa6-f176-4c58-acee-683d5e9d2ed5.'
            ),
            'external_url': 'http://www.ceda.ac.uk/',
            'license': (
                'Open Government Licence for Public Sector Information (UK) '
                'http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/'
            ),
            'filter': {
                'genre': 'DataGenreCC'
            },
            'aggs': [], 
        }
    ]

    COLLECTION = {
        "_type": "Collection",
        "uuid": "06d2de1c-559a-4e91-80ea-24aec53eca3f",
        "title": "Alimatic Research Unit (CRU) climate data",
        "description": "Global climate data from monthly observations collated by the University of East Anglia Climatic Research Unit",
        "rights": "Open Government Licence for Public Sector Information (UK)",
        "landingPage": "http://www.ceda.ac.uk/",
        "attribution": [DATASETS[0]['acknowledgement']],
        "subjects": ["Current datasets"],
        "categories": ["climate"],
        "BCCDataGenre": ["DataGenreCC"],
        "datasets": [],
    }

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['1800']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'category': self.CATEGORY,
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['1800']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': dsdef['title'].format(
                resolution=RESOLUTIONS['1800']['long'],
                **dsdef['filter']
            )
        }
        # collect some bits of metadata from data
        # all coverages have the same year and year_range
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        return ds_md


def main():
    gen = CRULayerMetadata()
    gen.main()


if __name__ == "__main__":
    main()
