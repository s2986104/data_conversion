#!/usr/bin/env python
import os.path
import re

from data_conversion.converter import BaseLayerMetadata
from data_conversion.coverage import gen_coverage_uuid
from data_conversion.vocabs import RESOLUTIONS, collection_by_id


class GeofabricLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'geofabric'
    # swift base url for this data
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'geofabric_layers'
    )

    DATASETS = [
        # Datasets
        {
            'title': 'Freshwater {btype} Data (Australia), {vname}, {res}'.format(btype=i[0].capitalize(), vname=i[3], res=RESOLUTIONS['9']['long']),
            'categories': ['environmental', i[2]],    # scientific type
            'domain': 'freshwater',
            'acknowledgement': (
                'Stein JL, Hutchinson MF, Stein JA (2014) A new stream and nested '
                'catchment framework for Australia. Hydrology and Earth System Sciences, '
                '18: 1917-1933. doi:10.5194/hess-18-1917-2014'
            ),
            'partof': [collection_by_id(i[5])],
            'filter': {
                'genre': i[4],
                'url': re.compile('^.*geofabric_{btype}_{dstype}.*\.tif$'.format(btype=i[0], dstype=i[1]))
            },
            'aggs': [],
        } for i in [
            # boundary type, dataset type, scientific type, variable name, genre, collection uuid 
            ('stream', 'climate', 'climate', 'current climate (1921-1995)', 'DataGenreCC', 'geofabric_stream_climate'),
            ('stream', 'vegetation', 'vegetation', 'Vegetation', 'DataGenreE', 'geofabric_stream_data'),
            ('stream', 'terrain', 'topography', 'Terrain', 'DataGenreE', 'geofabric_stream_data'),
            ('stream', 'substrate', 'substrate', 'Substrate', 'DataGenreE', 'geofabric_stream_data'),
            ('stream', 'population', 'human-impact', 'Population', 'DataGenreE', 'geofabric_stream_data'),
            ('stream', 'network', 'hydrology', 'Network', 'DataGenreE', 'geofabric_stream_data'),
            ('stream', 'landuse', 'landuse', 'Land Use', 'DataGenreE', 'geofabric_stream_data'),
            ('stream', 'connectivity', 'hydrology', 'Connectivity', 'DataGenreE', 'geofabric_stream_data'),
            ('catchment', 'climate', 'climate', 'current climate (1921-1995)', 'DataGenreCC', 'geofabric_catchment_climate'),
            ('catchment', 'vegetation', 'vegetation', 'Vegetation', 'DataGenreE', 'geofabric_catchment_data'),
            ('catchment', 'terrain', 'topography', 'Terrain', 'DataGenreE', 'geofabric_catchment_data'),
            ('catchment', 'substrate', 'substrate', 'Substrate', 'DataGenreE', 'geofabric_catchment_data'),
            ('catchment', 'population', 'human-impact', 'Population', 'DataGenreE', 'geofabric_catchment_data'),
            ('catchment', 'npp', 'vegetation', 'Net Primary Productivity', 'DataGenreE', 'geofabric_catchment_data'),
            ('catchment', 'landuse', 'landuse', 'Land Use', 'DataGenreE', 'geofabric_catchment_data'),
            ('catchment', 'rdi', 'human-impact', 'River Disturbance', 'DataGenreE', 'geofabric_catchment_data')
        ]
    ]

    def parse_filename(self, tiffile):
        return {
            'genre': 'DataGenreCC' if 'climate' in os.path.basename(tiffile).split('_') else 'DataGenreE',
            'resolution': RESOLUTIONS['9']['long']
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            # apply filter values as metadata
            # apply metadata bits from dsdef
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['9']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': dsdef['title'].format(resolution=RESOLUTIONS['9']['long'], **dsdef['filter'])
        }
        # collect some bits of metadata from data
        # all coverages have the same year and year_range
        if 'year' in coverages[0]['bccvl:metadata']:
            ds_md['year'] = coverages[0]['bccvl:metadata']['year']
            ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        ds_md['version'] = coverages[0]['bccvl:metadata']['version']
        return ds_md

    def get_genre(self, md): 
        return md['genre']


def main():
    gen = GeofabricLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
