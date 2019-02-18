from .climate import GCMS  # noqa
from .geotiff import PREDICTORS  # noqa

from .var_defs import (
    BIOCLIM_VAR_DEFS, WORLDCLIM_VAR_DEFS,
)


VAR_DEFS = {
    **BIOCLIM_VAR_DEFS,
    **WORLDCLIM_VAR_DEFS
}
