import argparse
import json
import os
import os.path

import yaml

from data_conversion.vocabs import VAR_DEFS


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('destdir', default=os.getcwd(), nargs='?', help='Output directory')
    parser.add_argument('--skos', action='store_true', help='Output in skos yaml')
    return parser.parse_args()

def main():
    opts = parse_args()
    opts.destdir = os.path.abspath(opts.destdir)

    if not opts.skos:
        with open(os.path.join(opts.destdir, 'variables.json'), 'w') as mdfile:
            # add datasets
            json.dump(list(VAR_DEFS.values()), mdfile, indent=2)
    else:
        out = [{
            'id': 'variables',
            'type': 'ConceptScheme'
        }]
        for var in VAR_DEFS.values():
            item = {
                'id': var['standard_name'],
                'type': 'Concept',
                'prefLabel': {
                    'en': var['long_name']
                },
                'measure_type': var['measure_type'],
                'units': var['units'],
            }
            for key in ('legend', 'description'):
                if key in var:
                    item[key] = var[key]
            out.append(item)
        with open(os.path.join(opts.destdir, 'variables.yaml'), 'w') as mdfile:
            # add datasets
            yaml.dump(out, mdfile, sort_keys=False)
