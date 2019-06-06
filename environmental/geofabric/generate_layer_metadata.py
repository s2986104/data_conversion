#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.coverage import gen_coverage_uuid
from data_conversion.vocabs import RESOLUTIONS, collection_by_id
from data_conversion.utils import RegExp

# Dataset full descriptions
DESCRIPTIONS = {
    'stream': {
        'climate': (
            'Aggregated climate data for the Australian continent between 1921-1995, '
            'generated using ANUCLIM version 6.1, for stream segments derived from the '
            'national 9 arcsec DEM and flow direction grid version 3. Stream segments '
            'refer to all grid cells comprising the stream segment and associated valley '
            'bottom.'
        ),
        'vegetation': (
            'Natural (pre-1750) and extant (present day) vegetation cover for stream '
            'segments across the Australian continent based on the NVIS Major Vegetation '
            'sub-groups version 3.1. Stream segments refer to all grid cells comprising '
            'the stream segment and associated valley bottom.'
        ),
        'terrain': (
            'Terrain data for stream segments across the Australian continent based on '
            'the 9" DEM of Australia version 3 (2008). Stream segments refer to all grid '
            'cells comprising the stream segment and associated valley bottom.'
        ),
        'substrate': (
            'Substrate data with soil hydrological characteristics and lithological '
            'composition for stream segments across the Australian continent based on '
            'the surface geology of Australia 1:1M. Stream segments refer to all grid '
            'cells comprising the stream segment and associated valley bottom.'
        ),
        'population': (
            'Population data for stream segments across the Australian continent based '
            'on the population density in 2006 (Australian Bureau of Statistics). Stream '
            'segments refer to all grid cells comprising the stream segment and associated '
            'valley bottom.'
        ),
        'network': (
            'Network parameters of habitat availability for stream segments and associated '
            'vallye bottoms and sub-catchments across the Australian continent based on '
            'AusHydro version 1.1.6 (Bureau of Meteorology, 2010).'
        ),
        'landuse': (
            'Land use data reflecting the proportion of 13 different land use activities '
            '(based on the tertiary land use classification by M. Stewardson, University '
            'of Melbourne, 2010) for stream segments across the Australian continent based '
            'on the Catchment-scale land use mapping for Australia (Bureau of Rural '
            'Sciences, 2009). Stream segments refer to all grid cells comprising the '
            'stream segment and associated valley bottom.'
        ),
        'connectivity': (
            'Connectivity parameters that indicate presence of major in-stream barriers '
            'including dams and waterfalls for stream segments and associated vallye '
            'bottoms and sub-catchments across the Australian continent based on '
            'AusHydro version 1.1.6 (Bureau of Meteorology, 2010).'
        )
    },
    'catchment': {
        'climate': (
            'Aggregated climate data for the Australian continent between 1921-1995, '
            'generated using ANUCLIM version 6.1, for catchments derived from the '
            'national 9 arcsec DEM and flow direction grid version 3. Catchments '
            'consist of all grid cells upstream of the center of the stream segment '
            'pour-point cell.'
        ),
        'vegetation': (
            'Natural (pre-1750) and extant (present day) vegetation cover for '
            'catchments across the Australian continent based on the NVIS Major '
            'Vegetation sub-groups version 3.1. Catchments consist of all grid '
            'cells upstream of the center of the stream segment pour-point cell.'
        ),
        'terrain': (
            'Terrain data for catchments across the Australian continent based on '
            'the 9" DEM of Australia version 3 (2008). Catchments consist of all '
            'grid cells upstream of the center of the stream segment pour-point cell.'
        ),
        'substrate': (
            'Substrate data with soil hydrological characteristics and lithological '
            'composition for catchments across the Australian continent based on the '
            'surface geology of Australia 1:1M. Catchments consist of all grid cells '
            'upstream of the center of the stream segment pour-point cell.'
        ),
        'population': (
            'Population data for catchments across the Australian continent based '
            'on the population density in 2006 (Australian Bureau of Statistics). '
            'Catchments consist of all grid cells upstream of the center of the '
            'stream segment pour-point cell.'
        ),
        'nppmon': (
            'Average of monthly mean net primary productivity (NPP) for catchments '
            'across the Australian continent based on Raupach et al. (2001). NPP is '
            'equal to plant photosynthesis less plant respiration, and reflects the '
            'carbon or biomass yield of the landscape, available for use by animals '
            'and humans. Catchments consist of all grid cells upstream of the center '
            'of the stream segment pour-point cell.'
        ),
        'nppann': (
            'Average of annual mean net primary productivity (NPP) for catchments '
            'across the Australian continent based on Raupach et al. (2001). NPP is '
            'equal to plant photosynthesis less plant respiration, and reflects the '
            'carbon or biomass yield of the landscape, available for use by animals '
            'and humans. Catchments consist of all grid cells upstream of the center '
            'of the stream segment pour-point cell.'
        ),
        'landuse': (
            'Land use data reflecting the proportion of 13 different land use activities '
            '(based on the tertiary land use classification by M. Stewardson, University '
            'of Melbourne, 2010) for catchments across the Australian continent based on '
            'the Catchment-scale land use mapping for Australia (Bureau of Rural Sciences, '
            '2009). Catchments consist of all grid cells upstream of the center of the '
            'stream segment pour-point cell.'
        ),
        'rdi': (
            'Indicators of pressure on stream ecosystems due to human activities derived '
            'using the method of Stein et al. (2002). The method couples geographical data, '
            'recording the extent and intensity of human activities known to impact on river '
            'condition, with a Digital Elevation Model (DEM) used for drainage analysis. The '
            'indices rank streams along a continuum from near-pristine to severely disturbed.'
        )
    }
}


class GeofabricLayerMetadata(BaseLayerMetadata):

    # TODO: should we rather set the id in DATASETS list?
    #       category as well?
    DATASET_ID = 'geofabric_{ref}'
    # swift base url for this data
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'geofabric_layers'
    )


    DATASETS = [
        # Datasets
        {
            'title': 'Freshwater {btype} Data (Australia), {vname}, {res}'.format(btype=i[0].capitalize(), vname=i[3].capitalize(), res=RESOLUTIONS['9']['long']),
            'description': DESCRIPTIONS.get(i[0]).get(i[1]),
            'categories': ['environmental', i[2]],    # scientific type
            'domain': 'freshwater',
            'acknowledgement': (
                'Stein JL, Hutchinson MF, Stein JA (2014) A new stream and nested '
                'catchment framework for Australia. Hydrology and Earth System Sciences, '
                '18: 1917-1933. doi:10.5194/hess-18-1917-2014'
            ),
            'partof': [collection_by_id(i[5])['uuid']],
            'filter': {
                'genre': i[4],
                'url': RegExp('^.*geofabric_{btype}_{dstype}.*\.tif$'.format(btype=i[0], dstype=i[1])) if i[6] is None else i[6]
            },
            'aggs': ['month'] if i[1] == 'nppmon' else [],
            'reference': i[0],
        } for i in [
            # boundary type, dataset type/layer id, scientific type, variable name, genre, collection uuid, regexp for filtering 
            ('stream', 'climate', 'climate', 'current climate (1921-1995)', 'DataGenreCC', 'geofabric_stream_climate', None),
            ('stream', 'vegetation', 'vegetation', 'Vegetation', 'DataGenreE', 'geofabric_stream_data', None),
            ('stream', 'terrain', 'topography', 'Terrain', 'DataGenreE', 'geofabric_stream_data', None),
            ('stream', 'substrate', 'substrate', 'Substrate', 'DataGenreE', 'geofabric_stream_data', None),
            ('stream', 'population', 'human-impact', 'Population', 'DataGenreE', 'geofabric_stream_data', None),
            ('stream', 'network', 'hydrology', 'Network', 'DataGenreE', 'geofabric_stream_data', None),
            ('stream', 'landuse', 'landuse', 'Land Use', 'DataGenreE', 'geofabric_stream_data', None),
            ('stream', 'connectivity', 'hydrology', 'Connectivity', 'DataGenreE', 'geofabric_stream_data', None),
            ('catchment', 'climate', 'climate', 'current climate (1921-1995)', 'DataGenreCC', 'geofabric_catchment_climate', None),
            ('catchment', 'vegetation', 'vegetation', 'Vegetation', 'DataGenreE', 'geofabric_catchment_data', None),
            ('catchment', 'terrain', 'topography', 'Terrain', 'DataGenreE', 'geofabric_catchment_data', None),
            ('catchment', 'substrate', 'substrate', 'Substrate', 'DataGenreE', 'geofabric_catchment_data', None),
            ('catchment', 'population', 'human-impact', 'Population', 'DataGenreE', 'geofabric_catchment_data', None),
            ('catchment', 'nppmon', 'vegetation', 'Net Primary Productivity (monthly)', 'DataGenreE', 'geofabric_catchment_data', RegExp('^.*geofabric_catchment_npp_nppmon.*\.tif$')),
            ('catchment', 'nppann', 'vegetation', 'Net Primary Productivity (annually)', 'DataGenreE', 'geofabric_catchment_data', RegExp('^.*geofabric_catchment_npp_nppann\.tif$')),
            ('catchment', 'landuse', 'landuse', 'Land Use', 'DataGenreE', 'geofabric_catchment_data', None),
            ('catchment', 'rdi', 'human-impact', 'River Disturbance', 'DataGenreE', 'geofabric_catchment_data', None)
        ]
    ]

    def parse_filename(self, tiffile):
        nameparts = os.path.basename(tiffile).split('_')
        return {
            'genre': 'DataGenreCC' if 'climate' in nameparts else 'DataGenreE',
            'reference': 'catchment' if 'catchment' in nameparts else 'stream',
            'resolution': RESOLUTIONS['9']['long']
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            # apply filter values as metadata
            # apply metadata bits from dsdef
            'description': dsdef['description'],
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
            'reference': dsdef.get('reference'),
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['9']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': dsdef['title']
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

    def cov_uuid(self, dscov):
        """
        Generate data/dataset uuid for dataset coverage
        """
        md = dscov['bccvl:metadata']
        return gen_coverage_uuid(dscov, self.DATASET_ID.format(ref=md.get('reference')))

def main():
    gen = GeofabricLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
