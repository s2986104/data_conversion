import json
import os.path

from .climate import GCMS  # noqa
from .geotiff import PREDICTORS  # noqa

from .var_defs import (
    BIOCLIM_VAR_DEFS, WORLDCLIM_VAR_DEFS, NSG_VAR_DEFS, NVIS_VAR_DEFS, AWAP_VAR_DEFS, ANUCLIM_VAR_DEFS, NDLC_VAR_DEFS, 
    VAST_VAR_DEFS, GPP_VAR_DEFS, MARINE_VAR_DEFS, MARSPEC_VAR_DEFS, AUSTOPO_VAR_DEFS
)


VAR_DEFS = {
    **BIOCLIM_VAR_DEFS,
    **WORLDCLIM_VAR_DEFS,
    **NSG_VAR_DEFS,	
    **NVIS_VAR_DEFS,
    **AWAP_VAR_DEFS,
    **ANUCLIM_VAR_DEFS,
    **NDLC_VAR_DEFS,
    **VAST_VAR_DEFS,
    **GPP_VAR_DEFS,
    **MARINE_VAR_DEFS,
    **MARSPEC_VAR_DEFS,
    **AUSTOPO_VAR_DEFS,
}

RESOLUTIONS = json.load(
    open(os.path.join(os.path.dirname(__file__), 'resolutions.json'), 'r')
)
