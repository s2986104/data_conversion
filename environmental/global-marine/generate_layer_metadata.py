#!/usr/bin/env python
import os.path
import re

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS

ACKNOWLEDGEMENT = (
    'Tyberghein L, Verbruggen H, Pauly K, Troupin C, Mineur F, De Clerck O (2012) Bio-ORACLE: '
    'A global environmental dataset for marine species distribution modelling. Global Ecology '
    'and Biogeography, 21: 272–281. \n'
    'Assis J, Tyberghein L, Bosh S, Verbruggen H, Serrão EA, De Clerck O (2017) Bio-ORACLE v2.0: '
    'Extending marine data layers for bioclimatic modelling. Global Ecology and Biogeography, 27: '
    '277-284.'
)


class GlobalMarineLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'global_marine'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'global_marine'
    )

    DATASETS = [
        # only one dataset in nsg
        {
            'title': 'Global marine surface data, {variable} ({yearrange}), {resolution}'.format(variable=i[0], yearrange=i[4], resolution=RESOLUTIONS['300']['long']),
            'acknowledgement': ACKNOWLEDGEMENT,
            'year': i[3],
            'license': (
                'Creative Commons Attribution 4.0 '
                'http://creativecommons.org/licenses/by/4.0'
            ),
            'external_url': 'http://www.bio-oracle.org/',
            'coluuid': '12aeac92c-77ff-4b44-8ae5-b26cb1c7ad58',
            'filter': {
                'genre': 'DataGenreE',
                'url': re.compile('^.*current_2007_Surface.{lid}.*\.tif$'.format(lid=i[1]))
            },
            'aggs': [],
            'category': i[2],
        } for i in [
            ('Cloud Cover', 'Cloud.Cover', 'physical', 2007, '2000-2014'), 
            ('Diffuse Attenuation', 'Diffuse.Attenuation', 'physical', 2007, '2000-2014'), 
            ('Sea Ice Concentration', 'Ice.Cover', 'physical', 2007, '2000-2014'), 
            ('Ice Thickness', 'Ice.Thickness', 'physical', 2007, '2000-2014'),
            ('Currents Velocity', 'Currents.Velocity', 'physical', 2007, '2000-2014'),
            ('Water Salinity', 'Salinity', 'physical', 2007, '2000-2014'),
            ('Water Temperature', 'Temperature', 'physical', 2007, '2000-2014'),
            ('Iron', 'Iron', 'nutrients', 2007, '2000-2014'),
            ('Calcite', 'Calcite', 'nutrients', 2007, '2000-2014'),
            ('Dissolved Molecular Oxygen', 'Dissolved.Oxygen', 'nutrients', 2007, '2000-2014'),
            ('Silicate', 'Silicate', 'nutrients', 2007, '2000-2014'),
            ('Phosphate', 'Phosphate', 'nutrients', 2007, '2000-2014'),
            ('Nitrate', 'Nitrate', 'nutrients', 2007, '2000-2014'),
            ('Primary Productivity', 'Primary.Productivity', 'biochemical', 2007, '2000-2014'),
            ('Photosynthetically Available Radiation', 'Par', 'biochemical', 2007, '2000-2014'),
            ('pH', 'Ph', 'biochemical', 2007, '2000-2014'),
            ('Phytoplankton', 'Phytoplankton', 'biochemical', 2007, '2000-2014'),
            ('Chlorophyll', 'Chlorophyll', 'biochemical', 2007, '2000-2014')
        ]
    ]

    FUTURE_DATASETS = [
        {
            'title': 'Global marine surface data, {variable} (year_range), emsc, {resolution}'.format(
                    variable=i[0], resolution=RESOLUTIONS['300']['long']
                ).replace('emsc', '{emsc}').replace('year_range', '{year_range}'),
            'acknowledgement': ACKNOWLEDGEMENT,
            'license': (
                'Creative Commons Attribution 4.0 '
                'http://creativecommons.org/licenses/by/4.0'
            ),
            'external_url': 'http://www.bio-oracle.org/',
            'coluuid': '12aeac92c-77ff-4b44-8ae5-b26cb1c7ad58',
            'filter': {
                'genre': 'DataGenreE',
                'url': re.compile('^.*rcp.*_Surface.{lid}.*\.tif$'.format(lid=i[1])),
                'emsc': None,
                'year': None
            },
            'aggs': [],
            'category': i[2],
        }
        for i in [
            ('Water Temperature', 'Temperature', 'physical'),
            ('Water Salinity', 'Salinity', 'physical'),
            ('Ice Thickness', 'Ice.Thickness', 'physical'),
            ('Currents Velocity', 'Currents.Velocity', 'physical')
        ]
    ]

    COLLECTION = [
        {
            "_type": "Collection",
            "uuid": "12aeac92c-77ff-4b44-8ae5-b26cb1c7ad58",
            "title": "Global Marine Environmental Data (Bio-ORACLE)",
            "description": "Bio-ORACLE, v2: a suite of geophysical, biotic and environmental data layers for surface waters of marine realms for current and future time periods.\n\nGeographic extent: Global\nYear range: 2000-2014, 2040-2050, 2090-2100\nResolution: {resolution}\nData layers: 280 layers across 26 datasets, including water temperature, salinity, currents velocity and chlorophyll A concentration".format(resolution=RESOLUTIONS['300']['long']),
            "rights": "CC-BY Attribution 4.0",
            "landingPage": "See <a http://www.bio-oracle.org/\">http://www.bio-oracle.org/</a>",
            "attribution": [
                "Tyberghein L, Verbruggen H, Pauly K, Troupin C, Mineur F, De Clerck O (2012) Bio-ORACLE: A global environmental dataset for marine species distribution modelling. Global Ecology and Biogeography, 21: 272–281.",
                "Assis J, Tyberghein L, Bosh S, Verbruggen H, Serrão EA, De Clerck O (2017) Bio-ORACLE v2.0: Extending marine data layers for bioclimatic modelling. Global Ecology and Biogeography, 27: 277-284."
            ],
            "subjects": ["Current datasets", "Future datasets"],
            "categories": ["environmental"],
            "BCCDataGenre": ["DataGenreE"],
            "datasets": [],
        }
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['300']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        # find year_range in coverages
        year_range = coverages[0]['bccvl:metadata']['year_range']
        year_range_str = '{}-{}'.format(year_range[0], year_range[1])
        ds_md = {
            'category': dsdef['category'],
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['300']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': dsdef.get('title').format(year_range=year_range_str, **dsdef['filter']),
        }
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = year_range

        if dsdef['filter'].get('emsc'):
            ds_md['emsc'] = dsdef['filter']['emsc']
        return ds_md

    def get_genre(self, md):
        return 'DataGenreE'

    def __init__(self):
        self.DATASETS.extend(self.FUTURE_DATASETS)


def main():
    gen = GlobalMarineLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
