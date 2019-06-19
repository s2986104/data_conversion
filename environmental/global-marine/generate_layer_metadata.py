#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS, collection_by_id
from data_conversion.utils import FilterType, RegExp

ACKNOWLEDGEMENT = (
    'Tyberghein L, Verbruggen H, Pauly K, Troupin C, Mineur F, De Clerck O (2012) Bio-ORACLE: '
    'A global environmental dataset for marine species distribution modelling. Global Ecology '
    'and Biogeography, 21: 272–281. \n'
    'Assis J, Tyberghein L, Bosh S, Verbruggen H, Serrão EA, De Clerck O (2017) Bio-ORACLE v2.0: '
    'Extending marine data layers for bioclimatic modelling. Global Ecology and Biogeography, 27: '
    '277-284.'
)

COMMON_DESC = (
    'Bio-ORACLE data layers are created from monthly pre-processed satellite and in situ observations. '
    'Bio-ORACLE is developed by a team of marine researchers from the Flanders Marine Institute (VLIZ), '
    'the University of Algarve, the University of Melbourne and Ghent University. '
    'Website: http://www.bio-oracle.org/'
)

# Dataset full descriptions
DESCRIPTIONS = {
    'Cloud Cover': (
        'Cloud cover indicates how much of the earth is covered by clouds. A bilinear '
        'interpolation was used to convert the data from 6 arcminutes to 5 arcminutes.'
    ),
    'Diffuse Attenuation': (
        'The diffuse attenuation coefficient is an indicator of water clarity. It expresses '
        'how deeply visible light in the blue to the green region of the spectrum penetrates '
        'into the water column.'
    ),
    'Sea Ice Concentration': (
        'Sea ice concentration refers to the area of sea ice relative to the total area of '
        'the ocean surface. '
    ),
    'Ice Thickness': (
        'Ice thickness in metres at the ocean surface. '
    ),
    'Currents Velocity': (
        'Measurements of current speeds at the ocean surface. '
    ),
    'Water Salinity': (
        'Salinity indicates the dissolved salt content in the ocean surface. '
    ),
    'Water Temperature': (
        'Sea surface temperature is the temperature of the topmost meter of the ocean water column. '
    ),
    'Iron': (
        'Micromole concentration of dissolved iron at the sea surface. '
    ),
    'Calcite': (
        'Calcite concentration indicates the mean concentration of calcite (CaCO3) in oceans. ',
    )
    'Dissolved Molecular Oxygen': (
        'Mole concentration of dissolved oxygen at the sea surface. '
    ),
    'Silicate': (
        'Mole concentration of silicate at the sea surface. '
    ),
    'Phosphate': (
        'Mole concentration of phosphate at the sea surface. '
    ),
    'Nitrate': (
        'Mole concentration of nitrate at the sea surface. '
    ),
    'Primary Productivity': (
        'Sea surface net primary productivity of carbon. '
    ),
    'Photosynthetically Available Radiation': (
        'Photosynthetically Available Radiation (PAR) indicates the quantum energy flux '
        'from the sun (in the spectral range 400-700 nm) reaching the ocean surface. '
    ),
    'pH': (
        'pH is an indicator of the acidity in the ocean, with lower values indicating '
        'more acid conditions and higher values more alkaline conditions. '
    ),
    'Phytoplankton': (
        'Mole concentration of phytoplankton expressed as carbon at the sea surface. '
    ),
    'Chlorophyll': (
        'Chlorophyll A concentration indicates the concentration of photosynthetic pigment '
        'chlorophyll A (the most common "green" chlorophyll) in oceans. Please note that in '
        'shallow water these values may reflect any kind of autotrophic biomass. '
    )
}


class GlobalMarineLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'global_marine'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'global_marine_layers'
    )

    DATASETS = [
        # only one dataset in nsg
        {
            'title': 'Global marine surface data, {variable} ({yearrange}), {resolution}'.format(
                variable=i[0], yearrange=i[4], resolution=RESOLUTIONS['300']['long']),
            'description': DESCRIPTIONS[i[0]] + COMMON_DESC,
            'categories': ['environmental', i[2]],
            'domain': 'marine',
            'acknowledgement': ACKNOWLEDGEMENT,
            'year': i[3],
            'license': (
                'Creative Commons Attribution 4.0 '
                'http://creativecommons.org/licenses/by/4.0'
            ),
            'external_url': 'http://www.bio-oracle.org/',
            'partof': [collection_by_id('global_marine_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE',
                'url': RegExp('^.*/Marine-Present-Surface_.*_Surface.{lid}.*\.tif$'.format(lid=i[1]))
            },
            'aggs': [],
        } for i in [
            ('Cloud Cover', 'Cloud.Cover', 'physical', 2007, '2000-2014'), 
            ('Diffuse Attenuation', 'Diffuse.Attenuation', 'physical', 2007, '2000-2014'), 
            ('Sea Ice Concentration', 'Ice.Cover', 'physical', 2007, '2000-2014'), 
            ('Ice Thickness', 'Ice.Thickness', 'physical', 2007, '2000-2014'),
            ('Currents Velocity', 'Current.Velocity', 'physical', 2007, '2000-2014'),
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
            'description': DESCRIPTIONS[i[0]] + COMMON_DESC,
            'categories': ['environmental', i[2]],
            'domain': 'marine',
            'acknowledgement': ACKNOWLEDGEMENT,
            'license': (
                'Creative Commons Attribution 4.0 '
                'http://creativecommons.org/licenses/by/4.0'
            ),
            'external_url': 'http://www.bio-oracle.org/',
            'partof': [collection_by_id('global_marine_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE',
                'url': RegExp('^.*/Marine-Future-Surface_.*_Surface.{lid}.*\.tif$'.format(lid=i[1])),
                'emsc': FilterType.DISCRIMINATOR,
                'year': FilterType.DISCRIMINATOR
            },
            'aggs': [],
        }
        for i in [
            ('Water Temperature', 'Temperature', 'physical'),
            ('Water Salinity', 'Salinity', 'physical'),
            ('Ice Thickness', 'Ice.Thickness', 'physical'),
            ('Currents Velocity', 'Current.Velocity', 'physical')
        ]
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
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
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
