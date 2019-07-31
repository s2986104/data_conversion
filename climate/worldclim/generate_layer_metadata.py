#!/usr/bin/env python
import os.path
import calendar

from data_conversion.converter import BaseLayerMetadata
from data_conversion.coverage import gen_coverage_uuid
from data_conversion.vocabs import RESOLUTIONS, collection_by_id
from data_conversion.utils import FilterType


class WorldClimLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'worldclim-{res}'
 
    # swift base url for this data
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'worldclim_layers'
    )

    DATASETS = [
        # current
        {
            # bio
            'title': 'WorldClim, current climate (1960-1990), {resolution}',
            'categories': ['environmental', 'climate'],
            'domain': 'terrestrial',
            'spatial_domain': 'Global',
            'acknowledgement': (
                'Hijmans RJ, Cameron SE, Parra JL, Jones PG, Jarvis A (2005) Very high '
                'resolution interpolated climate surfaces for global land areas. '
                'International Journal of Climatology 25: 1965-1978. doi:10.1002/joc.1276'
            ),
            'external_url': (
                'http://worldclim.org/'
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'partof': [collection_by_id('worldclim_climate_layers')['uuid']],            
            'filter': {
                'time_domain': 'Current',
                'month': FilterType.MISSING,
                'resolution': FilterType.DISCRIMINATOR
            },
            'aggs': []
        },
        {
            # alt
            'title': 'WorldClim, Altitude, {resolution}',
            'categories': ['environmental', 'topography'],
            'domain': 'terrestrial',
            'spatial_domain': 'Global',
            'acknowledgement': (
                'Hijmans RJ, Cameron SE, Parra JL, Jones PG, Jarvis A (2005) Very high '
                'resolution interpolated climate surfaces for global land areas. '
                'International Journal of Climatology 25: 1965-1978. doi:10.1002/joc.1276'
            ),
            'external_url': (
                'http://worldclim.org/'
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'partof': [collection_by_id('worldclim_altitude_layers')['uuid']],     
            'filter': {
                'time_domain': 'Current',
                'resolution': FilterType.DISCRIMINATOR
            },
            'aggs': []
        },
        {
            # monthly tmin, tmax, tmean, prec
            'title': 'WorldClim, current climate {monthname} (1960-1990), {resolution}',
            'categories': ['environmental', 'climate'],
            'domain': 'terrestrial',
            'spatial_domain': 'Global',
            'acknowledgement': (
                'Hijmans RJ, Cameron SE, Parra JL, Jones PG, Jarvis A (2005) Very high '
                'resolution interpolated climate surfaces for global land areas. '
                'International Journal of Climatology 25: 1965-1978. doi:10.1002/joc.1276'
            ),
            'external_url': (
                'http://worldclim.org/'
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'partof': [collection_by_id('worldclim_monthly_layers')['uuid']],            
            'filter': {
                'time_domain': 'Current',
                'month': FilterType.DISCRIMINATOR,
                'resolution': FilterType.DISCRIMINATOR
            },
            'aggs': ['month']
        },
        {
            # bio
            'title': 'WorldClim, future climate {year}, {gcm} {emsc}, {resolution}',
            'categories': ['environmental', 'climate'],
            'domain': 'terrestrial',
            'spatial_domain': 'Global',
            'acknowledgement': (
                'Hijmans RJ, Cameron SE, Parra JL, Jones PG, Jarvis A (2005) Very high '
                'resolution interpolated climate surfaces for global land areas. '
                'International Journal of Climatology 25: 1965-1978. doi:10.1002/joc.1276'
            ),
            'external_url': (
                'http://worldclim.org/'
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'partof': [collection_by_id('worldclim_climate_layers')['uuid']],            
            'filter': {
                'time_domain': 'Future',
                'month': FilterType.MISSING,
                'gcm': FilterType.DISCRIMINATOR,
                'emsc': FilterType.DISCRIMINATOR,
                'year': FilterType.DISCRIMINATOR,
                'resolution': FilterType.DISCRIMINATOR
            },
            'aggs': ['gcm', 'emsc', 'year'],
        },
        {
            # monthly tmin, tmax, tmean, prec
            'title': 'WorldClim, future climate {monthname} ({year}), {gcm} {emsc}, {resolution}',
            'categories': ['environmental', 'climate'],
            'domain': 'terrestrial',
            'spatial_domain': 'Global',
            'acknowledgement': (
                'Hijmans RJ, Cameron SE, Parra JL, Jones PG, Jarvis A (2005) Very high '
                'resolution interpolated climate surfaces for global land areas. '
                'International Journal of Climatology 25: 1965-1978. doi:10.1002/joc.1276'
            ),
            'external_url': (
                'http://worldclim.org/'
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'partof': [collection_by_id('worldclim_monthly_layers')['uuid']],            
            'filter': {
                'time_domain': 'Future',
                'month': FilterType.DISCRIMINATOR,
                'gcm': FilterType.DISCRIMINATOR,
                'emsc': FilterType.DISCRIMINATOR,
                'year': FilterType.DISCRIMINATOR,
                'resolution': FilterType.DISCRIMINATOR
            },
            'aggs': ['gcm', 'emsc', 'month', 'year'],
        },
    ]


    def parse_filename(self, tiffile):
        RESOLUTION_MAP = {  # udunits arc_minute / arcmin, UCUM/UOM: name: min_arc, symb: '
            '30s': '30',
            '2-5m': '150',
            '5m': '300',
            '10m': '600',
        }
        resolution = os.path.basename(os.path.dirname(os.path.dirname(tiffile)))
        return {
            'resolution': RESOLUTIONS[RESOLUTION_MAP[resolution]]['long'],
            'spatial_domain': 'Global',
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        month = dsdef['filter'].get('month')
        if month is not FilterType.MISSING and month:
            title = dsdef['title'].format(monthname = calendar.month_name[int(month)], **dsdef['filter'])
        else:
            title = dsdef['title'].format(**dsdef['filter'])
        ds_md = {
            # apply filter values as metadata
            # apply metadata bits from dsdef
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
            'spatial_domain': dsdef['spatial_domain'],
            'time_domain': dsdef['filter']['time_domain'],
            'resolution': dsdef['filter']['resolution'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': title
        }
        # collect some bits of metadata from data
        # all coverages have the same year and year_range
        ds_md['version'] = coverages[0]['bccvl:metadata']['version']

        if month is not FilterType.MISSING and month:
            ds_md['month'] = month
        if 'year' in coverages[0]['bccvl:metadata']:
            ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        if 'year_range' in coverages[0]['bccvl:metadata']:
            ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        if dsdef['filter'].get('emsc'):
            ds_md['emsc'] = dsdef['filter']['emsc']
        if dsdef['filter'].get('gcm'):
            ds_md['gcm'] = dsdef['filter']['gcm']
        return ds_md

    def cov_uuid(self, dscov):
        md = dscov['bccvl:metadata']
        return gen_coverage_uuid(dscov, self.DATASET_ID.format(res=md['resolution']))

    def get_time_domain(self, md):
        """
        Determine time_domain based on metadata from tiffile.
        """
        if 'emsc' in md and 'gcm' in md:
            # Future Climate
            return 'Future'
        # Altitude is env dataset, don't have year
        if 'year' not in md:
            return 'Current'
        else:
            # Current Climate
            return 'Current'

def main():
    gen = WorldClimLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
