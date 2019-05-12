#!/usr/bin/env python
import os.path
import re
import calendar

from data_conversion.converter import BaseLayerMetadata
from data_conversion.coverage import gen_coverage_uuid
from data_conversion.vocabs import RESOLUTIONS, collection_by_id
from data_conversion.utils import FilterType


class FparLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'fpar_{dstype}'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'fpar_layers'
    )

    DATASETS = [
        # Long-term fpar mean, min, max, cov
        {
            'title': 'Australia, MODIS-fPAR (2000-2014), {resolution}',
            'categories': ['environmental', 'vegetation'],
            'domain': 'terrestrial',
            'acknowledgement': (
                "Mackey B, Berry S, Hugh S, Ferrier S, Harwood TD, Williams KJ (2012) "
                "Ecosystem greenspots: identifying potential drought, fire, and "
                "climate‐change micro‐refuges. Ecological Applications, 22(6), 1852-1864."
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': '',
            'partof': [collection_by_id('fpar_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE',
                'month': FilterType.MISSING,
                'year': 2007,
                'url': re.compile('^.*_global_.*\.tif$')
            },
            'aggs': [],
            'dataset_type': 'global'
        },
        # Monthly datasets
        {
            'title': 'Australia, MODIS-fPAR {monthname} (2000-2014), {resolution}',
            'categories': ['environmental', 'vegetation'],
            'domain': 'terrestrial',
            'acknowledgement': (
                "Mackey B, Berry S, Hugh S, Ferrier S, Harwood TD, Williams KJ (2012) "
                "Ecosystem greenspots: identifying potential drought, fire, and "
                "climate‐change micro‐refuges. Ecological Applications, 22(6), 1852-1864."
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': '',
            'partof': [collection_by_id('fpar_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE',
                'month': FilterType.DISCRIMINATOR,
                'url': re.compile('^.*_monthly_.*\.tif$')
            },
            'aggs': [],
            'dataset_type': 'monthly'
        },
        # Grow-year datasets
        {
            'title': 'Australia, MODIS-fPAR ({year_range} Growing Year), {resolution}',
            'categories': ['environmental', 'vegetation'],
            'domain': 'terrestrial',
            'acknowledgement': (
                "Mackey B, Berry S, Hugh S, Ferrier S, Harwood TD, Williams KJ (2012) "
                "Ecosystem greenspots: identifying potential drought, fire, and "
                "climate‐change micro‐refuges. Ecological Applications, 22(6), 1852-1864."
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': '',
            'partof': [collection_by_id('fpar_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE',
                'month': FilterType.MISSING,
                'year': FilterType.DISCRIMINATOR,
                'url': re.compile('^.*_growyearly_.*\.tif$')
            },
            'aggs': [],
            'dataset_type': 'growyearly'
        },
        # Calendar year datasets
        {
            'title': 'Australia, MODIS-fPAR ({year}), {resolution}',
            'categories': ['environmental', 'vegetation'],
            'domain': 'terrestrial',
            'acknowledgement': (
                "Mackey B, Berry S, Hugh S, Ferrier S, Harwood TD, Williams KJ (2012) "
                "Ecosystem greenspots: identifying potential drought, fire, and "
                "climate‐change micro‐refuges. Ecological Applications, 22(6), 1852-1864."
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': '',
            'partof': [collection_by_id('fpar_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE',
                'month': FilterType.MISSING,
                'year': FilterType.DISCRIMINATOR,
                'url': re.compile('^.*_calyearly_.*\.tif$')
            },
            'aggs': [],
            'dataset_type': 'calyearly'
        },
        # fpar dataset per month per year
        {
            'title': 'Australia, MODIS-fPAR {monthname} {year}, {resolution}',
            'categories': ['environmental', 'vegetation'],
            'domain': 'terrestrial',
            'acknowledgement': (
                "Mackey B, Berry S, Hugh S, Ferrier S, Harwood TD, Williams KJ (2012) "
                "Ecosystem greenspots: identifying potential drought, fire, and "
                "climate‐change micro‐refuges. Ecological Applications, 22(6), 1852-1864."
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': '',
            'partof': [collection_by_id('fpar_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreE',
                'month': FilterType.DISCRIMINATOR,
                'year': FilterType.DISCRIMINATOR,
                'url': re.compile('^.*_fpar_.*\.tif$')
            },
            'aggs': [],
            'dataset_type': 'timeseries'
        },
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['9']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        month = dsdef['filter'].get('month')
        if month is not FilterType.MISSING and month:
            title = dsdef['title'].format(
                monthname = calendar.month_name[month],
                resolution=RESOLUTIONS['9']['long'],
                **dsdef['filter'])
        elif dsdef['dataset_type'] == 'growyearly':
            yrrange = coverages[0]['bccvl:metadata']['year_range']
            title = dsdef['title'].format(
                resolution=RESOLUTIONS['9']['long'],
                year_range='{}-{}'.format(yrrange[0], yrrange[1]),
                **dsdef['filter'])
        else:
            title = dsdef['title'].format(
                resolution=RESOLUTIONS['9']['long'],
                **dsdef['filter'])
        ds_md = {
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['9']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': title,
            'dataset_type': dsdef.get('dataset_type')
        }
        # find min/max years from the layer metadata.
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        if month is not FilterType.MISSING and month:
            ds_md['month'] = month
        return ds_md

    def get_genre(self, md):
        return 'DataGenreE'

    def cov_uuid(self, dscov):
        """
        Generate data/dataset uuid for dataset coverage
        """
        md = dscov['bccvl:metadata']
        return gen_coverage_uuid(dscov, self.DATASET_ID.format(dstype=md.get('dataset_type')))


def main():
    gen = FparLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
