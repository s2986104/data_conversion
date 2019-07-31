import argparse
import json
import os
import os.path

from data_conversion.vocabs import VAR_DEFS


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('destdir', default=os.getcwd(), nargs='?', help='Output directory')
    return parser.parse_args()

def main():
    opts = parse_args()
    opts.destdir = os.path.abspath(opts.destdir)

    with open(os.path.join(opts.destdir, 'variables.json'), 'w') as mdfile:
        # add datasets
        json.dump(list(VAR_DEFS.values()), mdfile, indent=2)
