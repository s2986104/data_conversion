#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS, collection_by_id


class CLIMONDLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'climond'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'climond_layers'
    )

    DATASETS = [
        # current
        {
            # bio
            'title': 'CliMond (global), current climate (1961 - 1990), {resolution}',
            'categories': ['environmental', 'climate'],
            'domain': 'terrestrial',
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
            'partof': [collection_by_id('climond_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreCC'
            },
            'aggs': [], 
        },
        {
            # bio
            'title': 'CliMond (global), Climate Projection, {emsc} based on {gcm}, {resolution} - {year}',
            'categories': ['environmental', 'climate'],
            'domain': 'terrestrial',
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
            'partof': [collection_by_id('climond_layers')['uuid']],
            'filter': {
                'genre': 'DataGenreFC',
                'gcm': None,
                'emsc': None,
                'year': None
            },
            'aggs': [], 
        }    
    ]

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['600']['long']
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
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
        # all coverages have the same year and year_range
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']

        if dsdef['filter'].get('emsc'):
            ds_md['emsc'] = dsdef['filter']['emsc']
        if dsdef['filter'].get('gcm'):
            ds_md['gcm'] = dsdef['filter']['gcm']
        return ds_md


def main():
    gen = CLIMONDLayerMetadata()
    gen.main()


if __name__ == "__main__":
    main()
