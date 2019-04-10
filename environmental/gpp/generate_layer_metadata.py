#!/usr/bin/env python
import os.path
import re

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS


class GPPLayerMetadata(BaseLayerMetadata):

    CATEGORY = 'vegetation'
    DATASET_ID = 'gpp'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'gpp'
    )

    DATASETS = [
        # Distinguisg dataset by layer name, and year
        {
            'title': 'Australia, Gross Primary Productivity (2000-2007), {resolution}',
            'acknowledgement': (
                "Roderick, M.; Farquhar, G.; Berry, S. et al. 2001. On the direct effect of clouds and atmospheric particles on the productivity and structure of vegetation. Oecolgoia, vol. 129, 21-30; ",
                "Berry, Sandra; Mackey, Brendan; Brown, Tiffany. 2007. Potential Applications of Remotely Sensed Vegetation Greenness to Habitat Analysis and the Conservation of Dispersive Fauna. Pacific Conservation Biology, Vol. 13, No. 2, [120]-127; ",
                "Mackey, Brendan; Berry, Sandra; Brown, Tiffany. 2008. Reconciling approaches to biogeographic regionalization: a systematic and generic framework examined with a case study of the Australian continent. Journal of Biogeography 35, 213-229; ",
                "Paget, MJ; King EA. 2008. MODIS Land data sets for the Australian region. CSIRO Marine and Atmospheric Research Internal Report 004"
            ),
            'year': 2003,
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': '',
            'filter': {
                'genre': 'DataGenreE',
                'url': re.compile('^.*gpp_maxmin_2000_2007.*\.tif$')
            },
            'aggs': [],
        },
        {
            'title': 'Australia, Gross Primary Productivity ({year}), {resolution}',
            'acknowledgement': (
                "Roderick, M.; Farquhar, G.; Berry, S. et al. 2001. On the direct effect of clouds and atmospheric particles on the productivity and structure of vegetation. Oecolgoia, vol. 129, 21-30; ",
                "Berry, Sandra; Mackey, Brendan; Brown, Tiffany. 2007. Potential Applications of Remotely Sensed Vegetation Greenness to Habitat Analysis and the Conservation of Dispersive Fauna. Pacific Conservation Biology, Vol. 13, No. 2, [120]-127; ",
                "Mackey, Brendan; Berry, Sandra; Brown, Tiffany. 2008. Reconciling approaches to biogeographic regionalization: a systematic and generic framework examined with a case study of the Australian continent. Journal of Biogeography 35, 213-229; ",
                "Paget, MJ; King EA. 2008. MODIS Land data sets for the Australian region. CSIRO Marine and Atmospheric Research Internal Report 004"
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': '',
            'filter': {
                'genre': 'DataGenreE',
                'url': re.compile('^.*gpp_year_means2000_2007.*_gppmean\.tif$'),
                'year': None
            },
            'aggs': [],
        }
    ]

    COLLECTION = {
        "_type": "Collection",
        "uuid": "f20868f0-f10d-4172-a532-afd6e1ba38e1",
        "title": "Australia Gross Primary Productivity",
        "description": "Australia Gross Primary Productivity\n\nGeographic extent: Australia\nYear range: 2000-2007\nResolution: {resolution}\nData layers: Annual mean, minimum and maximum Gross Primary Productivity, long-term average and CoV".format(resolution=RESOLUTIONS['9']['long']),
        "rights": "CC-BY Attribution 3.0",
        "landingPage": "",
        "attribution": [""],
        "subjects": ["Current datasets"],
        "categories": ["environmental"],
        "BCCDataGenre": ["DataGenreE"],
        "datasets": [],
    }

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['9']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'category': self.CATEGORY,
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['9']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': dsdef.get('title').format(
                resolution=RESOLUTIONS['9']['long'],
                **dsdef['filter']
            ),
        }
        # find min/max years from the layer metadata.
        ds_md['year'] = coverages[0]['bccvl:metadata']['year']
        ds_md['year_range'] = coverages[0]['bccvl:metadata']['year_range']
        return ds_md

    def get_genre(self, md):
        return 'DataGenreE'


def main():
    gen = GPPLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
