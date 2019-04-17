#!/usr/bin/env python
import os.path
import re

from data_conversion.converter import BaseLayerMetadata
from data_conversion.vocabs import RESOLUTIONS


class AusTopographyLayerMetadata(BaseLayerMetadata):

    CATEGORY = 'topography'
    DATASET_ID = 'aus-topography'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'aus-topography'
    )

    DATASETS = [
        # only one dataset in nsg
        {
            'title': 'Australia, Multi-resolution Valley Bottom Flatness (v1.0, 2013), {resolution}',
            'acknowledgement': (
                'Gallant J, Dowling T, Austin J (2013) Multi-resolution Valley Bottom Flatness (MrRTF, '
                '3" resolution). v1. CSIRO. Data Collection. https://doi.org/10.4225/08/512EF27AC3888'
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': 'https://data.csiro.au/dap/landingpage?pid=csiro:5681',
            'filter': {
                'genre': 'DataGenreE',
                'url': re.compile('^.*mrvbf.*\.tif$')
            },
            'aggs': [],
        },
        {
            'title': 'Australia, Multi-resolution Ridge Top Flatness (v1.0, 2013), {resolution}',
            'acknowledgement': (
                'Gallant J, Dowling T, Austin J (2013) Multi-resolution Ridge Top Flatness (MrRTF, '
                '3" resolution). v1. CSIRO. Data Collection. https://doi.org/10.4225/08/512EEA6332EEB'
            ),
            'license': (
                'Creative Commons Attribution 3.0 AU '
                'http://creativecommons.org/licenses/by/3.0/au'
            ),
            'external_url': 'https://data.csiro.au/dap/landingpage?pid=csiro:6239',
            'filter': {
                'genre': 'DataGenreE',
                'url': re.compile('^.*mrrtf.*\.tif$')
            },
            'aggs': [],
        }        
    ]

    COLLECTION = {
        "_type": "Collection",
        "uuid": "76f6b00a-706b-460b-98ac-67af601b348d",
        "title": "Australia Topography",
        "description": "Topography indices identifying areas of deposited material or high flat areas in Australia.\n\nGeographic extent: Australia\nYear range: 2000-2000\nResolution: {resolution}\nData layers: Multi-resolution Ridge Top Flatness, Multi-resolution Valley Bottom Flatness".format(resolution=RESOLUTIONS['3']['long']),
        "rights": "CC-BY Attribution 3.0",
        "landingPage": "See <a href=\"https://data.csiro.au/dap/\">https://data.csiro.au/dap/</a>",
        "attribution": [
            'Gallant J, Dowling T, Austin J (2013) Multi-resolution Ridge Top Flatness (MrRTF, 3" resolution). v1. CSIRO. Data Collection. https://doi.org/10.4225/08/512EEA6332EEB',
            'Gallant J, Dowling T, Austin J (2013) Multi-resolution Valley Bottom Flatness (MrRTF, 3" resolution). v1. CSIRO. Data Collection. https://doi.org/10.4225/08/512EF27AC3888'
        ],
        "subjects": ["Current datasets"],
        "categories": ["environmental"],
        "BCCDataGenre": ["DataGenreE"],
        "datasets": [],
    }

    def parse_filename(self, tiffile):
        return {
            'resolution': RESOLUTIONS['3']['long'],
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'category': self.CATEGORY,                  # scientific type
            'genre': dsdef['filter']['genre'],
            'resolution': RESOLUTIONS['3']['long'],
            'acknowledgement': dsdef.get('acknowledgment'),
            'external_url': dsdef.get('external_url'),
            'license': dsdef.get('license'),
            'title': dsdef.get('title').format(
                resolution=RESOLUTIONS['3']['long'],
                **dsdef['filter']
            ),
        }
        # find version from coverages
        ds_md['version'] = coverages[0]['bccvl:metadata']['version']
        return ds_md

    def get_genre(self, md):
        return 'DataGenreE'

    def get_rat_map(self, tiffile):
        if os.path.basename(tiffile) == 'aus-topography-90m-mrvbf_mrvbf.tif':
            return {
                'id': 'Threshold Slope (%)',
                'label': 'Interpretation',
                'value': 'VALUE'
            }
        elif os.path.basename(tiffile) == 'aus-topography-90m-mrrtf_mrrtf.tif':
            return {
                'id': 'Threshold Slope (%)',
                'label': 'Resolution (approx meter)',
                'value': 'VALUE'
            }

        return None

def main():
    gen = AusTopographyLayerMetadata()
    gen.main()

if __name__ == "__main__":
    main()
