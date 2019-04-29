#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.coverage import gen_coverage_uuid
from data_conversion.vocabs import RESOLUTIONS, collection_by_id


class AustraliaLayerMetadata(BaseLayerMetadata):

    # all datasets are in climate category
    CATEGORIES = ['environmental', 'climate']
    # TODO: should we rather set the id in DATASETS list?
    #       category as well?
    DATASET_ID = 'australia-{res}'
    # swift base url for this data
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'australia_layers'
    )

    DATASETS = [
        # current
        {
            # bio
            'title': 'Australia, Current Climate (1976-2005), {resolution}',
            'acknowledgement': (
                'Jones, D. A., Wang, W., & Fawcett, R. (2009). High-quality spatial '
                'climate data-sets for Australia. Australian Meteorological and '
                'Oceanographic Journal, 58(4), 233.'
            ),
            'partof': [collection_by_id('australia_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreCC',
                'resolution': None,
            },
            'aggs': [],
        },
        {
            'title': 'Australia, Climate Projection, {emsc} based on {gcm}, {resolution} - {year}',
            'acknowledgement': (
                'Vanderwal, Jeremy. (2012). All future climate layers for Australia - 5km '
                'resolution. James Cook University. [Data files] '
                'jcu.edu.au/tdh/collection/633b4ccd-2e78-459d-963c-e43d3c8a5ca1'
            ),
            'external_url': (
                'http://wallaceinitiative.org/climate_2012/tdhtools/Search/'
                'DataDownload.php?coverage=australia-5km'
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'partof': [collection_by_id('australia_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreFC',
                'gcm': None,
                'emsc': None,
                'year': None,
                'resolution': None,
            },
            'aggs': [],
        }
    ]

    def parse_filename(self, tiffile):
        RESOLUTION_MAP = {  # udunits arc_minute / arcmin, UCUM/UOM: name: min_arc, symb: '
            '5km': '150',
            '1km': '30',
        }
        resolution = os.path.basename(os.path.dirname(os.path.dirname(tiffile)))
        return {
            'resolution': RESOLUTIONS[RESOLUTION_MAP[resolution]]['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            # apply filter values as metadata
            # apply metadata bits from dsdef
            'category': self.CATEGORY,
            'genre': dsdef['filter']['genre'],
            'resolution': dsdef['filter']['resolution'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': dsdef['title'].format(**dsdef['filter'])
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

    def cov_uuid(self, dscov):
        md = dscov['bccvl:metadata']
        res = '1km' if '30' in md['resolution'] else '5km'
        return gen_coverage_uuid(dscov, self.DATASET_ID.format(res=res))


def main():
    gen = AustraliaLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
