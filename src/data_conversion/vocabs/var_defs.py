import json
import os.path


BIOCLIM_VAR_DEFS = json.load(open(os.path.join(os.path.dirname(__file__), 'bioclim_vars.json'), 'r'))

WORLDCLIM_VAR_DEFS = json.load(open(os.path.join(os.path.dirname(__file__), 'worldclim_vars.json'), 'r'))

NSG_VAR_DEFS = json.load(open(os.path.join(os.path.dirname(__file__), 'nsg_vars.json'), 'r'))
