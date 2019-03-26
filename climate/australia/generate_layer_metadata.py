#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.coverage import gen_coverage_uuid
from data_conversion.vocabs import RESOLUTIONS


class AustraliaLayerMetadata(BaseLayerMetadata):

    # all datasets are in climate category
    CATEGORY = 'climate'
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

    COLLECTION = {
        "_type": "Collection",
        "uuid": "1db9e574-2f14-11e9-b0ea-0242ac110002",
        "title": "Australia current and future climate data",
        "description": (
            "Current and future climate data for the Australian continent\n\n"
            "Geographic extent: Australia\nYear range: 1976-2005, 2015-2085\n"
            "Resolution: 30 arcsec (~1 km), 2.5 arcmin (~5 km)\nData layers: B01-19"
        ),
        "rights": "CC-BY Attribution 3.0",
        "landingPage": "See <a href=\"https://research.jcu.edu.au/researchdata/default/detail/a06a78f553e1452bcf007231f6204f04/\">https://research.jcu.edu.au/researchdata/default/detail/a06a78f553e1452bcf007231f6204f04/</a>",
        "attribution": ["Vanderwal, Jeremy. (2012). All future climate layers for Australia - 5km resolution. James Cook University."],
        "subjects": ["Current datasets", "Future datasets"],
        "categories": ["climate"],
        "BCCDataGenre": ["DataGenreCC", "DataGenreFC"],
        # will be created/filled by metadata generator
        "datasets": [],
    }

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
        if ds_md['genre'] == 'DataGenreCC':
            # all coverages have the same year and year_range
            ds_md['year'] = coverages[0]['bccvl:metadata']['year']
            ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        if dsdef['filter'].get('year'):
            # this is future?
            ds_md['gcm'] = dsdef['filter']['gcm']
            ds_md['emsc'] = dsdef['filter']['emsc']
            ds_md['year'] = coverages[0]['bccvl:metadata']['year']
            ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
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
