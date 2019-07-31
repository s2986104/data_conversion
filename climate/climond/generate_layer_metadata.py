#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS, collection_by_id
from data_conversion.utils import FilterType


# Dataset description
DESCRIPTION = (
    'CLIMOND Bioclimate Map Time-Series, 1975 - 2100.  30-year average mapped '
    'bioclimatic variables at global extent with 10 arcminute resolution.'
    'A set of 35 bioclimatic variables, calculated according to the WorldClim method. '
    'They are coded as follows: '
    'CLIMOND_01 = Annual Mean Temperature, '
    'CLIMOND_02 = Mean Diurnal Range, '
    'CLIMOND_03 = Isothermality (CLIMOND_02/CLIMOND_07), '
    'CLIMOND_04 = Temperature Seasonality, '
    'CLIMOND_05 = Max Temperature of Warmest Month, '
    'CLIMOND_06 = Min Temperature of Coldest Month, '
    'CLIMOND_07 = Temperature Annual Range (CLIMOND_05-CLIMOND_06), '
    'CLIMOND_08 = Mean Temperature of Wettest Quarter, '
    'CLIMOND_09 = Mean Temperature of Driest Quarter, '
    'CLIMOND_10 = Mean Temperature of Warmest Quarter, '
    'CLIMOND_11 = Mean Temperature of Coldest Quarter, '
    'CLIMOND_12 = Annual Precipitation, '
    'CLIMOND_13 = Precipitation of Wettest Month, '
    'CLIMOND_14 = Precipitation of Driest Month, '
    'CLIMOND_15 = Precipitation Seasonality (Coefficient of Variation), '
    'CLIMOND_16 = Precipitation of Wettest Quarter, '
    'CLIMOND_17 = Precipitation of Driest Quarter, '
    'CLIMOND_18 = Precipitation of Warmest Quarter, '
    'CLIMOND_19 = Precipitation of Coldest Quarter, '
    'CLIMOND_20 = Annual Mean Radiation, '
    'CLIMOND_21 = Highest Weekly Radiation, '
    'CLIMOND_22 = Lowest Weekly Radiation, '
    'CLIMOND_23 = Radiation Seasonality (Coefficient of Variation), '
    'CLIMOND_24 = Radiation of the Wettest Quarter, '
    'CLIMOND_25 = Radiation of the Driest Quarter, '
    'CLIMOND_26 = Radiation of the Warmest Quarter, '
    'CLIMOND_27 = Radiation of the Coldest Quarter, '
    'CLIMOND_28 = Annual Mean Moisture Index, '
    'CLIMOND_29 = Highest Weekly Moisture Index, '
    'CLIMOND_30 = Lowest Weekly Moisture Index, '
    'CLIMOND_31 = Moisture Index Seasonality (Coefficient of Variation), '
    'CLIMOND_32 = Mean Moisture Index of the Wettest Quarter, '
    'CLIMOND_33 = Mean Moisture Index of the Driest Quarter, '
    'CLIMOND_34 = Mean Moisture Index of the Warmest Quarter, '
    'CLIMOND_35 = Mean Moisture Index of the Coldest Quarter'
)


class CLIMONDLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'climond'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'climond_layers'
    )

    DATASETS = [
        # current
        {
            # bio
            'title': 'CliMond (global), current climate (1961 - 1990), {resolution}',
            'description': DESCRIPTION,
            'categories': ['environmental', 'climate'],
            'domain': 'terrestrial',
            'spatial_domain': 'Global',
            'acknowledgement': (
                'Kriticos, D.J., B.L. Webber, A. Leriche, N. Ota, I. Macadam, J. Bathols '
                '& J.K. Scott.  2012.  CliMond: global high-resolution historical and '
                'future scenario climate surfaces for bioclimatic modelling.  Methods '
                'in Ecology & Evolution, 3(1), 53 - 64.'
            ),
            'external_url': 'https://www.climond.org/ClimateData.aspx',
            'license': (
                'Creative Commons Attribution 3.0 AU'
                'https://creativecommons.org/licenses/by/3.0/au'
            ),
            'partof': [collection_by_id('climond_layers')['uuid']],
            'filter': {
                'time_domain': 'Current'
            },
            'aggs': [], 
        },
        {
            # bio
            'title': 'CliMond (global), Climate Projection, {emsc} based on {gcm}, {resolution} - {year}',
            'description': DESCRIPTION,
            'categories': ['environmental', 'climate'],
            'domain': 'terrestrial',
            'spatial_domain': 'Global',
            'acknowledgement': (
                'Kriticos, D.J., B.L. Webber, A. Leriche, N. Ota, I. Macadam, J. Bathols '
                '& J.K. Scott.  2012.  CliMond: global high-resolution historical and '
                'future scenario climate surfaces for bioclimatic modelling.  Methods '
                'in Ecology & Evolution, 3(1), 53 - 64.'
            ),
            'external_url': 'https://www.climond.org/ClimateData.aspx',
            'license': (
                'Creative Commons Attribution 3.0 AU'
                'https://creativecommons.org/licenses/by/3.0/au'
            ),
            'partof': [collection_by_id('climond_layers')['uuid']],
            'filter': {
                'time_domain': 'Future',
                'gcm': FilterType.DISCRIMINATOR,
                'emsc': FilterType.DISCRIMINATOR,
                'year': FilterType.DISCRIMINATOR
            },
            'aggs': [], 
        }    
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['600']['long'],
            'spatial_domain': 'Global',
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
            'spatial_domain': dsdef['spatial_domain'],
            'time_domain': dsdef['filter']['time_domain'],
            'resolution': RESOLUTIONS['600']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            # TODO: format title
            'title': dsdef['title'].format(
                resolution=RESOLUTIONS['600']['long'],
                **dsdef['filter']
            )
        }
        # collect some bits of metadata from data
        # all coverages have the same year and year_range
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']

        if dsdef['filter'].get('emsc'):
            ds_md['emsc'] = dsdef['filter']['emsc']
        if dsdef['filter'].get('gcm'):
            ds_md['gcm'] = dsdef['filter']['gcm']
        return ds_md


def main():
    gen = CLIMONDLayerMetadata()
    gen.main()


if __name__ == "__main__":
    main()
