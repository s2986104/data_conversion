#!/usr/bin/env python
import os.path

from data_conversion.converter import BaseLayerMetadata
from data_conversion.coverage import gen_coverage_uuid
from data_conversion.vocabs import RESOLUTIONS, collection_by_id
from data_conversion.utils import FilterType, RegExp


DS_DESCRIPTION = (
    'NaRCLIM Bioclimate Maps.  20-year average mapped bioclimatic variables for '
    'NSW, VIC & ACT with 9 arcsecond resolution. '
    'A set of 35 bioclimatic variables, calculated according to the WorldClim method. '
)

class NaRCLIMLayerMetadata(BaseLayerMetadata):

    DATASET_ID = 'narclim-{res}'
    SWIFT_CONTAINER = (
        'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
        'narclim_layers'
    )

    DATASETS = [
        # current
        {
            # bio
            'title': 'South-East Australia Current Climate, (2000), {resolution}',
            'description': DS_DESCRIPTION,
            'categories': ['environmental', 'climate'],
            'domain': 'terrestrial',
            'spatial_domain': 'Regional',
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
            'partof': [collection_by_id('narclim_layers')['uuid']],
            'filter': {
                'time_domain': 'Current',
                'url': RegExp(r'https://.*/.*NaR-Extent.*\.tif'),
                'resolution': FilterType.DISCRIMINATOR
            },
            'aggs': [], 
        },
        {
            # bio
            'title': 'South-East Australia Future Climate, ({year}), ({emsc}-R{rcm}) based on {gcm}, {resolution}',
            'description': DS_DESCRIPTION,
            'categories': ['environmental', 'climate'],
            'domain': 'terrestrial',
            'spatial_domain': 'Regional',
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
            'partof': [collection_by_id('narclim_layers')['uuid']],
            'filter': {
                'time_domain': 'Future',
                'gcm': FilterType.DISCRIMINATOR,
                'emsc': FilterType.DISCRIMINATOR,
                'rcm': FilterType.DISCRIMINATOR,
                'year': FilterType.DISCRIMINATOR,
                'resolution': FilterType.DISCRIMINATOR,
            },
            'aggs': [], 
        }    
    ]

    def parse_filename(self, tiffile):
        resolution = os.path.basename(os.path.dirname(os.path.dirname(tiffile)))
        return {
            'resolution': RESOLUTIONS[resolution]['long'],
            'spatial_domain': 'Regional',
        }

    def gen_dataset_metadata(self, dsdef, coverages):
        ds_md = {
            'categories': dsdef['categories'],
            'domain': dsdef['domain'],
            'spatial_domain': dsdef['spatial_domain'],
            'time_domain': dsdef['filter']['time_domain'],
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

        if dsdef['filter'].get('emsc'):
            ds_md['emsc'] = dsdef['filter']['emsc']
        if dsdef['filter'].get('gcm'):
            ds_md['gcm'] = dsdef['filter']['gcm']
        if dsdef['filter'].get('rcm'):
            ds_md['rcm'] = dsdef['filter']['rcm']
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
