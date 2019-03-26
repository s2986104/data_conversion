#!/usr/bin/env python
import os.path
import re

from data_conversion.converter import BaseLayerMetadata
from data_conversion.coverage import gen_coverage_uuid
from data_conversion.vocabs import RESOLUTIONS


class NaRCLIMLayerMetadata(BaseLayerMetadata):

    CATEGORY = 'climate'
    DATASET_ID = 'narclim-{res}'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'narclim'
    )

    DATASETS = [
        # current
        {
            # bio
            'title': 'South-East Australia Current Climate, (2000), {resolution}',
            'acknowledgement': (
                'Evans JP, Ji F, Lee C, Smith P, Argueso D and Fita L (2014) Design of '
                'a regional climate modelling projection ensemble experiment Geosci. '
                'Model Dev., 7, 621-629'
            ),
            'external_url': 'https://climatedata.environment.nsw.gov.au/',
            'license': (
                'Creative Commons Attribution 4.0'
                'https://creativecommons.org/licenses/by/4.0'
            ),
            'filter': {
                'genre': 'DataGenreCC',
                'url': re.compile(r'https://.*/.*NaR-Extent.*\.tif'),
                'resolution': None
            },
            'aggs': [], 
        },
        {
            # bio
            'title': 'South-East Australia Future Climate, ({year}), ({emsc}-R{rcm}) based on {gcm}, {resolution}',
            'acknowledgement': (
                'Evans JP, Ji F, Lee C, Smith P, Argueso D and Fita L (2014) Design of '
                'a regional climate modelling projection ensemble experiment Geosci. '
                'Model Dev., 7, 621-629'
            ),
            'external_url': 'https://climatedata.environment.nsw.gov.au/',
            'license': (
                'Creative Commons Attribution 4.0'
                'https://creativecommons.org/licenses/by/4.0'
            ),
            'filter': {
                'genre': 'DataGenreFC',
                'gcm': None,
                'emsc': None,
                'rcm': None,
                'year': None,
                'resolution': None,
            },
            'aggs': [], 
        }    
    ]

    COLLECTION = {
        "_type": "Collection",
        "uuid": "e7824f09-f1fd-4cbd-80dd-87ce80ba2ae8",
        "title": "NaRCLIM climate data",
        "description": "Current and future climate data for south-east Australia\n\nGeographic extent: South-east Australia\nYear range: 1990-2010, 2030, 2070\nResolution: 36 arcsec (~1km)\nData layers: B01-35",
        "rights": "CC-BY Attribution 4.0",
        "landingPage": "See <a href=\"https://climatedata.environment.nsw.gov.au/\">NSW Climate Data Portal</a>",
        "attribution": DATASETS[0]['acknowledgement'],
        "subjects": ["Current datasets", "Future datasets"],
        "categories": ["climate"],
        "BCCDataGenre": ["DataGenreCC", "DataGenreFC"],
        "datasets": [],
    }

    def parse_filename(self, tiffile):
        resolution = os.path.basename(os.path.dirname(os.path.dirname(tiffile)))
        resolution = '36' if '1km' in resolution else '9'
        return {
            'resolution': RESOLUTIONS[resolution]['long']
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'category': self.CATEGORY,
            'genre': dsdef['filter']['genre'],
            'resolution': dsdef['filter']['resolution'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            # TODO: format title
            'title': dsdef['title'].format(**dsdef['filter'])
        }
        # all coverages have the same year and year_range
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        if ds_md['genre'] == 'DataGenreFC':
            ds_md['emsc'] = dsdef['filter']['emsc']
            ds_md['gcm'] = dsdef['filter']['gcm']
            ds_md['rcm'] = dsdef['filter']['rcm']
        else:
            # current has emsc as well
            ds_md['emsc'] = coverages[0]['bccvl:metadata']['emsc']
        return ds_md

    def cov_uuid(self, dscov):
        md = dscov['bccvl:metadata']
        res = '1km' if '36' in md['resolution'] else '250m'
        return gen_coverage_uuid(dscov, self.DATASET_ID.format(res=res))


def main():
    gen = NaRCLIMLayerMetadata()
    gen.main()


if __name__ == "__main__":
    main()
