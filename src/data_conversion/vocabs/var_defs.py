import json
import os.path


BIOCLIM_VAR_DEFS = json.load(open(os.path.join(os.path.dirname(__file__), 'bioclim_vars.json'), 'r'))

WORLDCLIM_VAR_DEFS = json.load(open(os.path.join(os.path.dirname(__file__), 'worldclim_vars.json'), 'r'))

ANUCLIM_VAR_DEFS = json.load(open(os.path.join(os.path.dirname(__file__), 'anuclim_vars.json'), 'r'))

NSG_VAR_DEFS = json.load(open(os.path.join(os.path.dirname(__file__), 'nsg_vars.json'), 'r'))

AWAP_VAR_DEFS = json.load(open(os.path.join(os.path.dirname(__file__), 'awap_vars.json'), 'r'))

NVIS_VAR_DEFS = json.load(open(os.path.join(os.path.dirname(__file__), 'nvis_vars.json'), 'r'))

NDLC_VAR_DEFS = json.load(open(os.path.join(os.path.dirname(__file__), 'ndlc_vars.json'), 'r')) 

VAST_VAR_DEFS = json.load(open(os.path.join(os.path.dirname(__file__), 'vast_vars.json'), 'r')) 