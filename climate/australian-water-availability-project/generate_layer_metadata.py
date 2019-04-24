#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.coverage import gen_coverage_uuid
from data_conversion.vocabs import RESOLUTIONS


class AwapLayerMetadata(BaseLayerMetadata):

    # all datasets are of 'hydrology sciemtific type'
    CATEGORY = 'hydrology'
    # TODO: should we rather set the id in DATASETS list?
    #       category as well?
    DATASET_ID = 'awap'
    # swift base url for this data
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'awap_layers'
    )

    DATASETS = [
        # current
        {
            # bio
            'title': 'Australia, Water Availability ({year}), {resolution}',
            'acknowledgement': (
                'Raupach MR, PR Briggs, V Haverd, EA King, M Paget, CM Trudinger (2009), '
                'Australian Water Availability Project (AWAP): CSIRO Marine and '
                'Atmospheric Research Component: Final Report for Phase 3. CAWCR '
                'Technical Report No. 013. 67 pp.'
            ),
            'coluuid': '88b335a1-ce16-46a2-aa7c-5b1b8049ecd4',
            'filter': {
                'genre': 'DataGenreE',
                'year': None
            },
            'aggs': [],
        }
    ]

    COLLECTION = [
        {
            "_type": "Collection",
            "uuid": "88b335a1-ce16-46a2-aa7c-5b1b8049ecd4",
            "title": "Australian Water Availability Project",
            "description": (
                "Annual data about the state and trend of the terrestrial water balance in Australia.\n\n"
                "Geographic extent: Australia\nYear range: 1900-2013\n"
                "Resolution: 3 arcmin (~5 km)\nData layers: 36 layers including runoff, evaporation, soil moisture and heat flux"
            ),
            "rights": "CC-BY Attribution 3.0",
            "landingPage": "See <a href=\"http://www.csiro.au/awap/\">http://www.csiro.au/awap/</a>",
            "attribution": ["Raupach MR, PR Briggs, V Haverd, EA King, M Paget, CM Trudinger (2009), Australian Water Availability Project (AWAP): CSIRO Marine and Atmospheric Research Component: Final Report for Phase 3. CAWCR Technical Report No. 013. 67 pp."],
            "subjects": ["Current datasets"],
            "categories": ["environmental"],
            "BCCDataGenre": ["DataGenreE"],
            # will be created/filled by metadata generator
            "datasets": [],
        }
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['180']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            # apply filter values as metadata
            # apply metadata bits from dsdef
            'category': self.CATEGORY,
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['180']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': dsdef['title'].format(resolution=RESOLUTIONS['180']['long'], **dsdef['filter'])
        }
        # collect some bits of metadata from data
        # all coverages have the same year and year_range
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']ß
        return ds_md

    def get_genre(self, md):
        return 'DataGenreE'


def main():
    gen = AwapLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
