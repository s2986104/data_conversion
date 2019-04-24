#!/usr/bin/env python
import os.path
import re

from data_conversion.converter import BaseLayerMetadata
from data_conversion.coverage import gen_coverage_uuid
from data_conversion.vocabs import RESOLUTIONS


class GeofabricLayerMetadata(BaseLayerMetadata):

    # TODO: should we rather set the id in DATASETS list?
    #       category as well?
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
            'acknowledgement': (
                'Stein JL, Hutchinson MF, Stein JA (2014) A new stream and nested '
                'catchment framework for Australia. Hydrology and Earth System Sciences, '
                '18: 1917-1933. doi:10.5194/hess-18-1917-2014'
            ),
            'category': i[2],            # scientific type
            'coluuid': i[5],             # collection uuid
            'filter': {
                'genre': i[4],
                'url': re.compile('^.*geofabric_{btype}_{dstype}.*\.tif$'.format(btype=i[0], dstype=i[1]))
            },
            'aggs': [],
        } for i in [
            # boundary type, dataset type, scientific type, variable name, genre, collection uuid 
            ('stream', 'climate', 'climate', 'current climate (1921-1995)', 'DataGenreCC', '9720af6b-0aa4-4957-b3ca-db0d1ee60be0'),
            ('stream', 'vegetation', 'vegetation', 'Vegetation', 'DataGenreE', 'd35f05a9-4b27-48c8-b508-fb63115a6f3b'),
            ('stream', 'terrain', 'topography', 'Terrain', 'DataGenreE', 'd35f05a9-4b27-48c8-b508-fb63115a6f3b'),
            ('stream', 'substrate', 'substrate', 'Substrate', 'DataGenreE', 'd35f05a9-4b27-48c8-b508-fb63115a6f3b'),
            ('stream', 'population', 'human-impact', 'Population', 'DataGenreE', 'd35f05a9-4b27-48c8-b508-fb63115a6f3b'),
            ('stream', 'network', 'hydrology', 'Network', 'DataGenreE', 'd35f05a9-4b27-48c8-b508-fb63115a6f3b'),
            ('stream', 'landuse', 'landuse', 'Land Use', 'DataGenreE', 'd35f05a9-4b27-48c8-b508-fb63115a6f3b'),
            ('stream', 'connectivity', 'hydrology', 'Connectivity', 'DataGenreE', 'd35f05a9-4b27-48c8-b508-fb63115a6f3b'),
            ('catchment', 'climate', 'climate', 'current climate (1921-1995)', 'DataGenreCC', '9129e2f2-fee7-4c32-9e59-32832d5a90d3'),
            ('catchment', 'vegetation', 'vegetation', 'Vegetation', 'DataGenreE', '09f0b8e2-be00-400b-afaf-dff7504dcffd'),
            ('catchment', 'terrain', 'topography', 'Terrain', 'DataGenreE', '09f0b8e2-be00-400b-afaf-dff7504dcffd'),
            ('catchment', 'substrate', 'substrate', 'Substrate', 'DataGenreE', '09f0b8e2-be00-400b-afaf-dff7504dcffd'),
            ('catchment', 'population', 'human-impact', 'Population', 'DataGenreE', '09f0b8e2-be00-400b-afaf-dff7504dcffd'),
            ('catchment', 'npp', 'vegetation', 'Net Primary Productivity', 'DataGenreE', '09f0b8e2-be00-400b-afaf-dff7504dcffd'),
            ('catchment', 'landuse', 'landuse', 'Land Use', 'DataGenreE', '09f0b8e2-be00-400b-afaf-dff7504dcffd'),
            ('catchment', 'rdi', 'human-impact', 'River Disturbance', 'DataGenreE', '09f0b8e2-be00-400b-afaf-dff7504dcffd')
        ]
    ]

    COLLECTION = [
        {
            "_type": "Collection",
            "uuid": "9129e2f2-fee7-4c32-9e59-32832d5a90d3",
            "title": "Australia Catchment climate data",
            "description": (
                "A suite of climate layers calculated based on the Australian Hydrological Geospatial Fabric "
                "(‘Geofabric’) network that represents freshwater catchments.\n\n"
                "Geographic extent: Australia\nYear range: 1921-1995\n"
                "Resolution: 9 arcsec (~250 m)\nData layers: B01, 05, 06, 08, 09, 11, 12, 16-20, Growth index, Growth index seasonality, Rainfall erosivity"
            ),
            "rights": "CC-BY Attribution 4.0",
            "landingPage": "See <a href=\"https://data.gov.au/dataset/national-environmental-stream-attributes-v1-1-5\">https://data.gov.au/dataset/national-environmental-stream-attributes-v1-1-5</a>",
            "attribution": ["Stein JL, Hutchinson MF, Stein JA (2014) A new stream and nested catchment framework for Australia. Hydrology and Earth System Sciences, 18: 1917-1933. doi:10.5194/hess-18-1917-2014"],
            "subjects": ["Current datasets"],
            "categories": ["climate"],
            "BCCDataGenre": ["DataGenreCC"],
            # will be created/filled by metadata generator
            "datasets": [],
        },
        {
            "_type": "Collection",
            "uuid": "9720af6b-0aa4-4957-b3ca-db0d1ee60be0",
            "title": "Australia Stream climate data",
            "description": (
                "A suite of climate layers calculated based on the Australian Hydrological Geospatial Fabric "
                "(‘Geofabric’) network that represents freshwater catchments.\n\n"
                "Geographic extent: Australia\nYear range: 1921-1995\n"
                "Resolution: 9 arcsec (~250 m)\nData layers: B01, 05, 06, 08, 09, 11, 12, 16-20, Growth index, Growth index seasonality, Rainfall erosivity"
            ),
            "rights": "CC-BY Attribution 4.0",
            "landingPage": "See <a href=\"https://data.gov.au/dataset/national-environmental-stream-attributes-v1-1-5\">https://data.gov.au/dataset/national-environmental-stream-attributes-v1-1-5</a>",
            "attribution": ["Stein JL, Hutchinson MF, Stein JA (2014) A new stream and nested catchment framework for Australia. Hydrology and Earth System Sciences, 18: 1917-1933. doi:10.5194/hess-18-1917-2014"],
            "subjects": ["Current datasets"],
            "categories": ["climate"],
            "BCCDataGenre": ["DataGenreCC"],
            # will be created/filled by metadata generator
            "datasets": [],
        },
        {
            "_type": "Collection",
            "uuid": "09f0b8e2-be00-400b-afaf-dff7504dcffd",
            "title": "Australia Catchment Data",
            "description": (
                "A suite of environmental layers calculated for freshwater catchments based on "
                "the Australian Hydrological Geospatial Fabric (‘Geofabric’) network.\n\n"
                "Geographic extent: Australia\nYear range: \n"
                "Resolution: 9 arcsec (~250 m)\nData layers: 176 layers across 7 datasets, including vegetation, river disturbance, landuse and population"
            ),
            "rights": "CC-BY Attribution 4.0",
            "landingPage": "See <a href=\"https://data.gov.au/dataset/national-environmental-stream-attributes-v1-1-5\">https://data.gov.au/dataset/national-environmental-stream-attributes-v1-1-5</a>",
            "attribution": ["Stein JL, Hutchinson MF, Stein JA (2014) A new stream and nested catchment framework for Australia. Hydrology and Earth System Sciences, 18: 1917-1933. doi:10.5194/hess-18-1917-2014"],
            "subjects": ["Current datasets"],
            "categories": ["environmental"],
            "BCCDataGenre": ["DataGenreE"],
            # will be created/filled by metadata generator
            "datasets": [],
        },
        {
            "_type": "Collection",
            "uuid": "d35f05a9-4b27-48c8-b508-fb63115a6f3b",
            "title": "Australia Stream Data",
            "description": (
                "A suite of environmental layers calculated for freshwater catchments based on "
                "the Australian Hydrological Geospatial Fabric (‘Geofabric’) network.\n\n"
                "Geographic extent: Australia\nYear range: \n"
                "Resolution: 9 arcsec (~250 m)\nData layers: 176 layers across 7 datasets, including vegetation, river disturbance, landuse and population"
            ),
            "rights": "CC-BY Attribution 4.0",
            "landingPage": "See <a href=\"https://data.gov.au/dataset/national-environmental-stream-attributes-v1-1-5\">https://data.gov.au/dataset/national-environmental-stream-attributes-v1-1-5</a>",
            "attribution": ["Stein JL, Hutchinson MF, Stein JA (2014) A new stream and nested catchment framework for Australia. Hydrology and Earth System Sciences, 18: 1917-1933. doi:10.5194/hess-18-1917-2014"],
            "subjects": ["Current datasets"],
            "categories": ["environmental"],
            "BCCDataGenre": ["DataGenreE"],
            # will be created/filled by metadata generator
            "datasets": [],
        }
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
            'category': dsdef.get('category'),
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
