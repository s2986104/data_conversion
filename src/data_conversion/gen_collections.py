import argparse
import json
import os
import os.path

from data_conversion.vocabs import COLLECTIONS


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('destdir', default=os.getcwd(), nargs='?', help='Output directory')
    return parser.parse_args()

def main():
    opts = parse_args()
    opts.destdir = os.path.abspath(opts.destdir)

    with open(os.path.join(opts.destdir, 'collections.json'), 'w') as mdfile:
        # add datasets
        json.dump(list(COLLECTIONS.values()), mdfile, indent=2)
