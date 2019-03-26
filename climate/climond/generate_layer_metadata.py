#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS


class CLIMONDLayerMetadata(BaseLayerMetadata):

    CATEGORY = 'climate'
    DATASET_ID = 'climond'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'climond'
    )

    DATASETS = [
        # current
        {
            # bio
            'title': 'CliMond (global), current climate (1961 - 1990), {resolution}',
            'acknowledgement': (
                'Kriticos, D.J., B.L. Webber, A. Leriche, N. Ota, I. Macadam, J. Bathols '
                '& J.K. Scott.  2012.  CliMond: global high-resolution historical and '
                'future scenario climate surfaces for bioclimatic modelling.  Methods '
                'in Ecology & Evolution, 3(1), 53 - 64.'
            ),
            'external_url': 'https://www.climond.org/ClimateData.aspx',
            'license': (
                'Creative Commons Attribution 3.0 AU'
                'https://creativecommons.org/licenses/by/3.0/au'
            ),
            'filter': {
                'genre': 'DataGenreCC'
            },
            'aggs': [], 
        },
        {
            # bio
            'title': 'CliMond (global), Climate Projection, {emsc} based on {gcm}, {resolution} - {year}',
            'acknowledgement': (
                'Kriticos, D.J., B.L. Webber, A. Leriche, N. Ota, I. Macadam, J. Bathols '
                '& J.K. Scott.  2012.  CliMond: global high-resolution historical and '
                'future scenario climate surfaces for bioclimatic modelling.  Methods '
                'in Ecology & Evolution, 3(1), 53 - 64.'
            ),
            'external_url': 'https://www.climond.org/ClimateData.aspx',
            'license': (
                'Creative Commons Attribution 3.0 AU'
                'https://creativecommons.org/licenses/by/3.0/au'
            ),
            'filter': {
                'genre': 'DataGenreFC',
                'gcm': None,
                'emsc': None,
                'year': None
            },
            'aggs': [], 
        }    
    ]

    COLLECTION = {
        "_type": "Collection",
        "uuid": "9a865673-a8f1-4e05-9f4d-8b950b8206b9",
        "title": "CliMond climate data",
        "description": "Global current and future climate data\n\nGeographic extent: Global\nYear range: 1961-1990, 2030, 2050, 2070, 2090, 2100\nResolution: {resolution}\nData layers: B01-35".format(resolution=RESOLUTIONS['600']['long']),
        "rights": "CC-BY Attribution 3.0 AU",
        "landingPage": "See <a href=\"https://www.climond.org/ClimateData.aspx\">CliMond Climate Data</a>",
        "attribution": DATASETS[0]['acknowledgement'],
        "subjects": ["Current datasets", "Future datasets"],
        "categories": ["climate"],
        "BCCDataGenre": ["DataGenreCC", "DataGenreFC"],
        "datasets": [],
    }

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['600']['long']
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'category': self.CATEGORY,
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['600']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            # TODO: format title
            'title': dsdef['title'].format(
                resolution=RESOLUTIONS['600']['long'],
                **dsdef['filter']
            )
        }
        # collect some bits of metadata from data
        if dsdef['filter']['genre'] == 'DataGenreCC':
            # all coverages have the same year and year_range
            ds_md['year'] = coverages[0]['bccvl:metadata']['year']
            ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        else:
            # future
            ds_md['gcm'] = dsdef['filter']['gcm']
            ds_md['emsc'] = dsdef['filter']['emsc']
            ds_md['year'] = coverages[0]['bccvl:metadata']['year']
            ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        return ds_md


def main():
    gen = CLIMONDLayerMetadata()
    gen.main()


if __name__ == "__main__":
    main()
