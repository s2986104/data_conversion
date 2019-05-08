#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS, collection_by_id


class PetAridityLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'global_pet_and_aridity'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'global_pet_and_aridity_layers'
    )

    DATASETS = [
        # only one dataset
        {
            'title': 'Global, Potential Evapo-Transpiration and Aridity (1950-2000), {resolution}',
            'categories': ['environmental', 'hydrology'],
            'domain': 'terrestrial',
            'acknowledgement': (
                'Zomer RJ, Trabucco A, Bossio DA, van Straaten O, Verchot LV, 2008. '
                'Climate Change Mitigation: A Spatial Analysis of Global Land Suitability '
                'for Clean Development Mechanism Afforestation and Reforestation. Agric. '
                'Ecosystems and Envir. 126: 67-80.',
                'Zomer RJ, Bossio DA, Trabucco A, Yuanjie L, Gupta DC & Singh VP, 2007. '
                'Trees and Water: Smallholder Agroforestry on Irrigated Lands in Northern '
                'India. Colombo, Sri Lanka: International Water Management Institute. '
                'pp 45. (IWMI Research Report 122).'
            ),
            'year': 1975,
            'license': (
                'Creative Commons Attribution 4.0'
                'http://creativecommons.org/licenses/by/4.0'
            ),
            'external_url': 'http://www.cgiar-csi.org/data/global-aridity-and-pet-database',
            'partof': [collection_by_id('global_pet_and_aridity_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE',
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


def main():
    gen = PetAridityLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
