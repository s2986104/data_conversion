#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS, collection_by_id
from data_conversion.utils import FilterType


class AccuClimLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'accuclim'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'accuclim_layers'
    )

    DATASETS = [
        # current
        {
            # bio
            'title': 'accuCLIM (Wet Tropics Australia), 30-year average either side of ({year}), {resolution}',
            'description': (
                'accuCLIM Bioclimate Map Time-Series, 1965 - 2000.  30-year average mapped '
                'bioclimatic variables for the Australian Wet Tropics, statistically downscaled '
                'according to key environmental and topographic factors, at 9 arcsecond resolution.'
                'A set of 7 bioclimatic variables, calculated according to the WorldClim method. '
                'They are coded as follows: '
                'ccuCLIM_01 = Annual Mean Temperature, '
                'accuCLIM_02 = Mean Diurnal Range, '
                'accuCLIM_03 = Isothermality (accuCLIM_02/accuCLIM_07), '
                'accuCLIM_04 = Temperature Seasonality, '
                'accuCLIM_05 = Max Temperature of Warmest Month, '
                'accuCLIM_06 = Min Temperature of Coldest Month, '
                'accuCLIM_07 = Temperature Annual Range (accuCLIM_05-accuCLIM_06).'
            ),
            'categories': ['environmental', 'climate'],
            'domain': 'terrestrial',
            'spatial_domain': 'Regional',
            'acknowledgement': (
                'Storlie, C.J., Phillips, B.L., VanDerWal, J.J., and Williams, S.E. (2013) '
                'Improved spatial estimates of climate predict patchier species distributions. '
                'Diversity and Distributions, 19 (9). pp. 1106-1113.'
            ),
            'external_url': 'https://researchdata.ands.org.au/accuclim-30-year-heritage-area/654267',
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'partof': [collection_by_id('accuclim_layers')['uuid']],
            'filter': {
                'time_domain': 'Current',
                'year': FilterType.DISCRIMINATOR
            },
            'aggs': [], 
        }
    ]

    def parse_filename(self, tiffile):
        return {
            # all 9 arcsec
            'resolution': RESOLUTIONS['9']['long'],
            'spatial_domain': 'Regional',
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
            'spatial_domain': dsdef['spatial_domain'],
            'time_domain': dsdef['filter']['time_domain'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'resolution': RESOLUTIONS['9']['long'],
            'title': dsdef['title'].format(
                resolution=RESOLUTIONS['9']['long'],
                **dsdef['filter'])
        }
        # collect some bits of metadata from data
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        return ds_md


def main():
    gen = AccuClimLayerMetadata()
    gen.main()


if __name__ == "__main__":
    main()
