#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.coverage import gen_coverage_uuid
from data_conversion.vocabs import RESOLUTIONS, collection_by_id
from data_conversion.utils import FilterType


class AwapLayerMetadata(BaseLayerMetadata):

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
            'categories': ['environmental', 'hydrology'],
            'domain': 'terrestrial',
            'spatial_domain': 'Australia',
            'acknowledgement': (
                'Raupach MR, PR Briggs, V Haverd, EA King, M Paget, CM Trudinger (2009), '
                'Australian Water Availability Project (AWAP): CSIRO Marine and '
                'Atmospheric Research Component: Final Report for Phase 3. CAWCR '
                'Technical Report No. 013. 67 pp.'
            ),
            'partof': [collection_by_id('awap_layers')['uuid']],
            'filter': {
                'time_domain': 'Current',
                'year': FilterType.DISCRIMINATOR
            },
            'aggs': [],
        }
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['180']['long'],
            'spatial_domain': 'Australia',
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            # apply filter values as metadata
            # apply metadata bits from dsdef
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
            'spatial_domain': dsdef['spatial_domain'],
            'time_domain': dsdef['filter']['time_domain'],
            'resolution': RESOLUTIONS['180']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': dsdef['title'].format(resolution=RESOLUTIONS['180']['long'], **dsdef['filter'])
        }
        # collect some bits of metadata from data
        # all coverages have the same year and year_range
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        return ds_md

    def get_time_domain(self, md):
        return 'Current'


def main():
    gen = AwapLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
