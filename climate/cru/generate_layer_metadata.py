#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS, collection_by_id

#Dataset 
DS_DESCRIPTION = (
    'Climatic Research Unit (CRU) Bioclimate Map Time-Series, 1915 - 1995. 30-year '
    'average mapped bioclimatic variables at global extent with 30 arcminute resolution.'
    'A set of 19 bioclimatic variables, calculated according to the WorldClim method. '
    'They are coded as follows: '
    'CRUCLIM_01 = Annual Mean Temperature, '
    'CRUCLIM_02 = Mean Diurnal Range, '
    'CRUCLIM_03 = Isothermality (CRUCLIM_02/CRUCLIM_07), '
    'CRUCLIM_04 = Temperature Seasonality, '
    'CRUCLIM_05 = Max Temperature of Warmest Month, '
    'CRUCLIM_06 = Min Temperature of Coldest Month, '
    'CRUCLIM_07 = Temperature Annual Range (CRUCLIM_05-CRUCLIM_06), '
    'CRUCLIM_08 = Mean Temperature of Wettest Quarter, '
    'CRUCLIM_09 = Mean Temperature of Driest Quarter, '
    'CRUCLIM_10 = Mean Temperature of Warmest Quarter, '
    'CRUCLIM_11 = Mean Temperature of Coldest Quarter, '
    'CRUCLIM_12 = Annual Precipitation, '
    'CRUCLIM_13 = Precipitation of Wettest Month, '
    'CRUCLIM_14 = Precipitation of Driest Month, '
    'CRUCLIM_15 = Precipitation Seasonality (Coefficient of Variation), '
    'CRUCLIM_16 = Precipitation of Wettest Quarter, '
    'CRUCLIM_17 = Precipitation of Driest Quarter, '
    'CRUCLIM_18 = Precipitation of Warmest Quarter, '
    'CRUCLIM_19 = Precipitation of Coldest Quarter'
)



class CRULayerMetadata(BaseLayerMetadata):

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
            'description': DS_DESCRIPTION,
            'categories': ['environmental', 'climate'],
            'domain': 'terrestrial',
            'acknowledgement': (
                'University of East Anglia Climatic Research Unit; Harris, I.C.; '
                'Jones, P.D. (2015): CRU TS3.23: Climatic Research Unit (CRU) '
                'Time-Series (TS) Version 3.23 of High Resolution Gridded Data '
                'of Month-by-month Variation in Climate (Jan. 1901- Dec. 2014). '
                'Centre for Environmental Data Analysis, 09 November 2015. '
                'doi:10.5285/4c7fdfa6-f176-4c58-acee-683d5e9d2ed5.'
            ),
            'external_url': 'http://www.ceda.ac.uk/',
            'coluuid': '06d2de1c-559a-4e91-80ea-24aec53eca3f',
            'license': (
                'Open Government Licence for Public Sector Information (UK) '
                'http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/'
            ),
            'partof': [collection_by_id('cruclim_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreCC'
            },
            'aggs': [], 
        }
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['1800']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
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
