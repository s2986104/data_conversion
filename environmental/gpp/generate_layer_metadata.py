#!/usr/bin/env python
import os.path
import re

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS, collection_by_id
from data_conversion.utils import FilterType


class GPPLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'gpp'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'gpp_layers'
    )

    DATASETS = [
        # Distinguisg dataset by layer name, and year
        {
            'title': 'Australia, Gross Primary Productivity (2000-2007), {resolution}',
            'categories': ['environmental', 'vegetation'],
            'domain': 'terrestrial',
            'acknowledgement': (
                "Roderick, M.; Farquhar, G.; Berry, S. et al. 2001. On the direct effect of clouds and atmospheric particles on the productivity and structure of vegetation. Oecolgoia, vol. 129, 21-30; ",
                "Berry, Sandra; Mackey, Brendan; Brown, Tiffany. 2007. Potential Applications of Remotely Sensed Vegetation Greenness to Habitat Analysis and the Conservation of Dispersive Fauna. Pacific Conservation Biology, Vol. 13, No. 2, [120]-127; ",
                "Mackey, Brendan; Berry, Sandra; Brown, Tiffany. 2008. Reconciling approaches to biogeographic regionalization: a systematic and generic framework examined with a case study of the Australian continent. Journal of Biogeography 35, 213-229; ",
                "Paget, MJ; King EA. 2008. MODIS Land data sets for the Australian region. CSIRO Marine and Atmospheric Research Internal Report 004"
            ),
            'year': 2003,
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': '',
            'partof': [collection_by_id('gpp_layers')['uuid']],
            'filter': {
                'time_domain': 'Current',
                'url': re.compile('^.*gpp_maxmin_2000_2007.*\.tif$')
            },
            'aggs': [],
        },
        {
            'title': 'Australia, Gross Primary Productivity ({year}), {resolution}',
            'categories': ['environmental', 'vegetation'],
            'domain': 'terrestrial',
            'acknowledgement': (
                "Roderick, M.; Farquhar, G.; Berry, S. et al. 2001. On the direct effect of clouds and atmospheric particles on the productivity and structure of vegetation. Oecolgoia, vol. 129, 21-30; ",
                "Berry, Sandra; Mackey, Brendan; Brown, Tiffany. 2007. Potential Applications of Remotely Sensed Vegetation Greenness to Habitat Analysis and the Conservation of Dispersive Fauna. Pacific Conservation Biology, Vol. 13, No. 2, [120]-127; ",
                "Mackey, Brendan; Berry, Sandra; Brown, Tiffany. 2008. Reconciling approaches to biogeographic regionalization: a systematic and generic framework examined with a case study of the Australian continent. Journal of Biogeography 35, 213-229; ",
                "Paget, MJ; King EA. 2008. MODIS Land data sets for the Australian region. CSIRO Marine and Atmospheric Research Internal Report 004"
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': '',
            'partof': [collection_by_id('gpp_layers')['uuid']],
            'filter': {
                'time_domain': 'Current',
                'url': re.compile('^.*gpp_year_means2000_2007.*_gppmean\.tif$'),
                'year': FilterType.DISCRIMINATOR
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
            'time_domain': dsdef['filter']['time_domain'],
            'resolution': RESOLUTIONS['9']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': dsdef.get('title').format(
                resolution=RESOLUTIONS['9']['long'],
                **dsdef['filter']
            ),
        }
        # find min/max years from the layer metadata.
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        return ds_md

    def get_time_domain(self, md):
        return 'Current'


def main():
    gen = GPPLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
