import argparse
import json
import os
import os.path

from data_conversion.vocabs import COLLECTIONS


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('destdir', default=os.getcwd())
    return parser.parse_args()

    def main(self):
        # TODO: we need a mode to just update as existing json file without parsing
        #       all tiff files. This would be useful to just update titles and
        #       things.
        #       could probably also be done in a separate one off script?
        opts = self.parse_args()
        opts.destdir = os.path.abspath(opts.destdir)

        datajson = os.path.join(opts.srcdir, 'data.json')

        
def main():
    opts = self.parse_args()
    opts.destdir = os.path.abspath(opts.destdir)

    with open(os.path.join(opts.destdir, 'collections.json'), 'w') as mdfile:
        # add datasets
        json.dump(list(COLLECTIONS.values()), mdfile, indent=2)
