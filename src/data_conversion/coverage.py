import copy
import logging
from math import isnan
import os.path
import uuid

from osgeo import gdal, gdal_array, osr

from data_conversion.utils import transform_pixel, open_gdal_dataset
from data_conversion.vocabs import VAR_DEFS


GDAL_JSON_TYPE_MAP = {
    gdal.GDT_Unknown: "undefined",
    gdal.GDT_Byte: "integer",
    gdal.GDT_UInt16: "integer",
    gdal.GDT_Int16: "integer",
    gdal.GDT_UInt32: "integer",
    gdal.GDT_Int32: "integer",
    gdal.GDT_Float32: "float",
    gdal.GDT_Float64: "float",
    gdal.GDT_CInt16: "integer",
    gdal.GDT_CInt32: "integer",
    gdal.GDT_CFloat32: "float",
    gdal.GDT_CFloat64: "float",
}


TIFF_MD_MAP = {
    'emission_scenario': 'emsc',
    'general_circulation_models': 'gcm',
    'regional_climate_model': 'rcm',
}


NAMESPACE_UUID = uuid.uuid5(uuid.NAMESPACE_DNS, 'datamanger.bccvl.org.au')


def gen_tif_metadata(tiffile, srcdir, swiftcontainer):
    """read metadata from tiffile
    """
    md = {}
    ds = open_gdal_dataset(tiffile)
    dsmd = ds.GetMetadata()
    # put all known global tiff metadata into md
    # TODO: we have tied the attribute name to the axis name ... (emsc, gcm, etc...)
    #       maybe we should rethink how we name axes, and potentially give
    #       them different names than the attribute
    for key, value in dsmd.items():
        key = TIFF_MD_MAP.get(key, key)
        if key in ('AREA_OR_POINT',):
            # keys to skip
            continue
        if key in ('year', 'month'):
            md[key] = int(value)
        elif key in ('year_range',):
            md[key] = [int(x) for x in value.split('-')]
        else:
            md[key] = value

    # build swift url:
    # TODO: should we do this here? or in collection specific code?
    relpath = os.path.relpath(tiffile, srcdir)
    md['url'] = os.path.join(swiftcontainer, relpath)
    # check if we use the GeoTiff driver
    if ds.GetDriver().ShortName == 'GTiff':
        # get list of auxilary files
        auxfiles = set(ds.GetFileList()) - set([tiffile])
        md['auxfiles'] = [
            # TODO: should probably check for file type here
            {
                'type': 'PAMDataset',  # this is a auxilary metadata like RAT
                # construct auxfile path relative to tiffile
                'path': os.path.relpath(aux, os.path.dirname(tiffile))
            }
            for aux in auxfiles
        ]
    return md


# TODO: this should probably be customised per collection to make
#       it easier to produce stable uuids
#       or maybe jsut parameterise the fileds that go into the uuid?
def gen_coverage_uuid(cov, identifier):
    # generate predictable uuid
    # kind + 'id' + time_domain + variable names + emsc + gcm + year + month
    # if any of these identifiers change, the uuid will be different
    md = cov['bccvl:metadata']
    # determine if Data or Dataset; dataset has title, data do not have.
    # TODO: better detection of Dataset or not
    kind = 'Data' if len(cov['parameters']) == 1 and 'title' not in cov['bccvl:metadata'] else 'Dataset'
    parts = [
        kind, identifier, md['time_domain'],
        ''.join(sorted(cov['parameters'].keys())),
    ]
    for key in ('emsc', 'gcm', 'rcm', 'year', 'month'):
        parts.append(str(md.get(key, '')))

    uid = uuid.uuid5(NAMESPACE_UUID, ''.join(parts))
    return str(uid)


def gen_tif_coverage(tiffile, url, ratmap=None):
    ds = open_gdal_dataset(tiffile)
    return gen_cov_json(ds, url, ratmap)


def gen_dataset_coverage(coverages, aggs=[]):
    return {
        "type": "Coverage",
        "domain": get_dataset_cov_domain(coverages, aggs),
        "parameters": gen_dataset_cov_parameters(coverages, aggs),
        "ranges": {},
        "rangeAlternates": gen_dataset_cov_range_alternates(coverages, aggs)
    }


def validate_bbox(bbox, crs):
    # bbox is within area of use of crs
    # if within a small tolerance we assume arounding error
    # which may come from the original data (sometimes shifted slighty)
    # or due to our conversion
    # if error is bigger than tolerance raise an exception
    log = logging.getLogger(__name__)
    tol = 0.0001
    use = crs.GetAreaOfUse()
    if use is not None:
        if use.south_lat_degree > bbox['bottom']:
            if abs(use.south_lat_degree - bbox['bottom']) > tol:
                raise Exception('BBOX bottom out of bonuds {}'.format(bbox['bottom']))
            log.warn('Adjusting BBOX bottom {} to {}'.format(bbox['bottom'], use.south_lat_degree))
            bbox['bottom'] = use.south_lat_degree
        if use.north_lat_degree < bbox['top']:
            if abs(use.north_lat_degree - bbox['top']) > tol:
                raise Exception('BBOX top out of bonuds {}'.format(bbox['top']))
            log.warn('Adjusting BBOX top {} to {}'.format(bbox['top'], use.north_lat_degree))
            bbox['top'] = use.north_lat_degree
        if use.west_lon_degree > bbox['left']:
            if abs(use.west_lon_degree - bbox['left']) > tol:
                raise Exception('BBOX left out of bonuds {}'.format(bbox['left']))
            log.warn('Adjusting BBOX left {} to {}'.format(bbox['left'], use.west_lon_degree))
            bbox['left'] = use.west_lon_degree
        if use.east_lon_degree < bbox['right']:
            if abs(use.east_lon_degree - bbox['right']) > tol:
                raise Exception('BBOX right out of bonuds {}'.format(bbox['right']))
            log.warn('Adjusting BBOX right {} to {}'.format(bbox['right'], use.east_lon_degree))
            bbox['right'] = use.east_lon_degree
    return bbox


def get_coverage_extent(coverage):
    # calc extent in WGS84 for x/y axes in given CRS
    axes = coverage['domain']['axes']
    src_crs = osr.SpatialReference(
        coverage['domain']['referencing'][0]['system']['wkt']
    )
    dst_crs = osr.SpatialReference()
    # dst crs 4326 has lat/lon axis order
    dst_crs.ImportFromEPSGA(4326)
    
    x_size = (axes['x']['stop'] - axes['x']['start']) / (axes['x']['num'] - 1) / 2
    y_size = abs((axes['y']['stop'] - axes['y']['start']) / (axes['y']['num'] - 1) / 2)
    # we have to subtract/add half a step to get full extent
    # Can I get rid of sorted here? (now that axes should be sorted already)
    xs = sorted([axes['x']['start'], axes['x']['stop']])
    ys = sorted([axes['y']['start'], axes['y']['stop']])
    xs = [xs[0] - x_size, xs[1] + x_size]
    ys = [ys[0] - y_size, ys[1] + y_size]
    # force our src_crs to lat/lon coordinate order (so that we can call TransformPoint in a consistent manner independet of axis order)
    src_crs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    transform = osr.CoordinateTransformation(src_crs, dst_crs)
    # epsg:4326 axis order is lat/lon
    # TODO: Warning, this code required GDAL >= 3
    bottom_left = transform.TransformPoint(xs[0], ys[0])
    top_right = transform.TransformPoint(xs[1], ys[1])
    bbox = {
        # 5 decimal digits is roughly 1m
        'bottom': round(bottom_left[0], 5),
        'left': round(bottom_left[1], 5),
        'top': round(top_right[0], 5),
        'right': round(top_right[1], 5),
    }
    # check area_of_use
    bbox = validate_bbox(bbox, dst_crs)
    return bbox
    

def gen_cov_domain_axes(ds):
    # calculate raster mid points
    # axes ranges are defined as closed interval
    # bottom left
    # TODO: replace transform_pikel with gdal.ApplyGeoTransform(gt, x, y)
    p0 = transform_pixel(ds, 0.5, 0.5)
    # top right
    p1 = transform_pixel(ds, ds.RasterXSize - 0.5, ds.RasterYSize - 0.5)
    # TODO: do I need to do anything about image being upside / down?

    return {
        "x": {"start": min(p0[0], p1[0]), "stop": max(p0[0], p1[0]), "num": ds.RasterXSize},
        "y": {"start": min(p0[1], p1[1]), "stop": max(p0[1], p1[1]), "num": ds.RasterYSize},
        # tiffs are anly 2 dimensional, so no need to add any further axes
    }


def gen_cov_referencing(ds):
    crs = osr.SpatialReference(ds.GetProjection())
    crs_type = "ProjectedCRS" if crs.IsProjected() else "GeographicCRS"
    # assumes that projection has an EPSG code
    crs_id = "http://www.opengis.net/def/crs/EPSG/0/{}".format(
        crs.GetAttrValue('AUTHORITY', 1)
    )
    crs_wkt = crs.ExportToWkt()
    return [{
        "coordinates": ["x", "y"],
        "system": {
            "type": crs_type,
            "id": crs_id,
            "wkt": crs_wkt,
        }
    }]


def gen_cov_domain(ds):
    return {
        "type": "Domain",
        "domainType": "Grid",
        "axes": gen_cov_domain_axes(ds),
        "referencing": gen_cov_referencing(ds),
    }

def gen_cov_categories(band, ratmap):
    # generate categories from raster band's RAT
    categories = []
    categoryEncoding = {}
    rat = band.GetDefaultRAT()
    if rat and ratmap:
        icolcount=rat.GetColumnCount()
        cols=[]
        for icol in range(icolcount):
            cols.append(rat.GetNameOfCol(icol))
        indexes = [cols.index(ratmap[key]) for key in ['id', 'label', 'value']]

        #Write out each row.
        irowcount = rat.GetRowCount()
        for irow in range(irowcount):
            values=[]
            for icol in range(icolcount):
                itype=rat.GetTypeOfCol(icol)
                if itype==gdal.GFT_Integer:
                    value=rat.GetValueAsInt(irow,icol)
                elif itype==gdal.GFT_Real:
                    value=rat.GetValueAsDouble(irow,icol)
                else:
                    value=rat.GetValueAsString(irow,icol)
                values.append(value)
            categories.append({
                    'id': values[indexes[0]],
                    'label': {
                        "en": values[indexes[1]]
                    }
                })
            categoryEncoding[values[indexes[0]]] = values[indexes[2]]
    return categories, categoryEncoding

def gen_cov_parameters(ds, ratmap=None):
    # All our datasets have only one band
    band = ds.GetRasterBand(1)
    bandmd = band.GetMetadata_Dict()
    categories, categoryEncoding = gen_cov_categories(band, ratmap)
    parameters = {
        bandmd['standard_name']: {
            "type": "Parameter",
            # "id": bandmd['standard_name'], common identifier?
            # "label": {"en": "..."}, only if different to label in
            #          observedProperty
            # "description": "{"en": ".."}, optional
            "observedProperty": {
                "label": {"en": bandmd['long_name']},
                # "id": bandmd['standard_name'],
                # "description": {"en", "..."}, optional
                # "categories": [  # only for categorical data
                #     {
                #         "id": "",
                #         "label": {"en": ""},
                #         # "description":,"",
                #     },
                # ],
                # "categoryEncoding": {  # optional if there are categories
                #     "<category.id>": <int>|[<int>, <int>,...]
                # }
            },
            "unit": {  # categories don't have units
                # "id": "",  # optional
                # "label": {"en": ""},  # at least label or symbol is
                #          required
                "symbol": {
                    "value": band.GetUnitType(),
                    "type" : "http://www.opengis.net/def/uom/UCUM/",
                }, 
            }
        }
    }
    if categories:
        parameters[bandmd['standard_name']]['observedProperty']['categories'] = categories
        parameters[bandmd['standard_name']]['categoryEncoding'] = categoryEncoding
    else:
        # TODO: do I need to adjust statistics by offset/scale?
        # apply min/max value range and nodata value to observedProperty
        (min, max, mean, stddev) = band.GetStatistics(False, False)
        # force correct data type GetNoDataValue always returns a float
        dtype = gdal_array.GDALTypeCodeToNumericTypeCode(band.DataType)
        # TODO: this will happily convert almost anything to some dtype
        #       e.g. np.int16(65535) == -1 (similar with floats)
        nodata = dtype(band.GetNoDataValue()).item()
        if isnan(nodata):
            nodata = None
        parameters[bandmd['standard_name']]['observedProperty']['dmgr:statistics'] = {
            'min': dtype(min).item(),
            'max': dtype(max).item(),
            'mean': mean,
            'stddev': stddev
        }
        parameters[bandmd['standard_name']]['observedProperty']['dmgr:nodata'] = nodata
    # check VAR_DEFS and add dmgr:legen dor observedProperty
    var_def = VAR_DEFS.get(bandmd['standard_name'])
    if var_def and 'legend' in var_def:
        parameters[bandmd['standard_name']]['observedProperty']['dmgr:legend'] = var_def['legend']
    return parameters


def gen_cov_ranges(ds):
    # it's binary tiff, potentially very large,
    # so our ranges are empty
    return {}


def gen_cov_range_alternates(ds, url):
    # single band tiff
    # I don't know of any standard prefix to describe rangeAlternates,
    # so let's use one that is hopefully not woll-known.
    # dmgr: for Datamanager
    band = ds.GetRasterBand(1)
    bandmd = band.GetMetadata_Dict()
    return {
        # TODO: convert min/max/nodata to actual dtypes.item() from band.DataType
        "dmgr:tiff": {
            bandmd['standard_name']: {
                "type": "dmgr:TIFF2DArray",
                "datatype": GDAL_JSON_TYPE_MAP[band.DataType],
                "axisNames": ["y", "x"],
                "shape": [band.YSize, band.XSize],
                "dmgr:band": 1,
                "dmgr:offset": band.GetOffset(),
                "dmgr:scale": band.GetScale(),
                "dmgr:missingValue": band.GetNoDataValue(),
                "dmgr:min": band.GetMinimum(),
                "dmgr:max": band.GetMaximum(),
                "dmgr:datatype": gdal.GetDataTypeName(band.DataType),
                "url": url,
                # "urlTemplate": "http://exampl.com/dataservice/{y}/{x}"
            }
        }
    }


def gen_cov_json(ds, url, ratmap=None):
    return {
        "type": "Coverage",
        "domain": gen_cov_domain(ds),
        "parameters": gen_cov_parameters(ds, ratmap),
        "ranges": gen_cov_ranges(ds),
        "rangeAlternates": gen_cov_range_alternates(ds, url)
    }


def get_dataset_cov_domain(coverages, aggs):
    return {
        "type": "Domain",
        "domainType": "Grid",
        "axes": gen_dataset_cov_domain_axes(coverages, aggs),
        "referencing": gen_dataset_cov_referencing(coverages, aggs),
    }


def gen_dataset_cov_domain_axes(coverages, aggs):
    # x/y can be taken from first coverage
    axes = {
        "x": coverages[0]['domain']['axes']['x'],
        "y": coverages[0]['domain']['axes']['y'],
    }
    # go through all aggregations which will become axes as well
    for agg in aggs:
        if not all(agg in c['bccvl:metadata'] for c in coverages):
            # sanity check
            # some coverages are missing the agg key ... we have to fail here
            raise Exception("Some coverages are missing aggregation key: {}".format(agg))
        values = {c['bccvl:metadata'][agg] for c in coverages}
        axes[agg] = {"values": sorted(values)}
        # special handling for years
        if agg == 'year':
            bounds = {
                tuple(c['bccvl:metadata']['year_range']) for c in coverages
            }
            axes[agg]['bounds'] = [
                item for sublist in sorted(bounds) for item in sublist
            ]
    return axes


def gen_dataset_cov_referencing(coverages, aggs):
    # just copy from first coverage, we don't add any projected axes
    return coverages[0]['domain']['referencing']


def gen_dataset_cov_parameters(coverages, aggs):
    log = logging.getLogger(__name__)
    # copy all uniquely named parameters
    parameters = {}
    for coverage in coverages:
        for key, value in coverage['parameters'].items():
            if key in parameters:
                if not aggs:
                    # sanity check before we loose data in coverage json
                    raise Exception('Overriding exisiting Parameter "{}". Most likely some kind of naming or aggregation.'.format(
                        key
                    ))
                # update stats
                # get rid of mean and stddev, and pick min/max accordingly
                # TODO: what to do about nodata?
                #       issue warning if nodata is different for now
                # TODO: aggregating categories should probably viladate for equality as well
                stats = parameters[key]['observedProperty'].get('dmgr:statistics')
                if stats:
                    stats.pop('mean', None)
                    stats.pop('stddev', None)
                    stats['min'] = min(stats['min'], value['observedProperty']['dmgr:statistics']['min'])
                    stats['max'] = max(stats['max'], value['observedProperty']['dmgr:statistics']['max'])
                if parameters[key]['observedProperty'].get('dmgr:nodata') != value['observedProperty'].get('dmgr:nodata'):
                    log.warn('NoData does not match in parameter aggregation: {}'.format(key))
                continue
            # we have to deepcopy here, because we may modify the dict on aggregation
            parameters[key] = copy.deepcopy(value)
    return parameters


def gen_dataset_cov_range_alternates(coverages, aggs):
    # build dmgr:tiff dict
    dmgr_tiff = {}
    if len(aggs) == 0:
        for cov in coverages:
            # one range for each variable/parameter
            for key in cov['rangeAlternates']['dmgr:tiff'].keys():
                dmgr_tiff[key] = cov['rangeAlternates']['dmgr:tiff'][key]
    else:
        # TODO: we should not re-calc this to get the shape for aggregated axes
        axes = gen_dataset_cov_domain_axes(coverages, aggs)
        aggs_shape = [len(axes[x]['values']) for x in aggs]
        for cov in coverages:
            # check if variable is already defined
            for key in cov['rangeAlternates']['dmgr:tiff'].keys():
                if key not in dmgr_tiff:
                    # create variable
                    range_alt = cov['rangeAlternates']['dmgr:tiff'][key]
                    dmgr_tiff[key] = {
                        "type": "dmgr:TIFF2DAggregation",
                        "datatype": range_alt['datatype'],
                        "axisNames": aggs + range_alt['axisNames'],
                        "shape": aggs_shape + range_alt['shape'],
                        "tiles": [],
                    }
                # ok variable exists, let's add this tile
                tile = [cov['bccvl:metadata'][x] for x in aggs]  # + [None] * len(cov['rangeAlternates']['dmgr:tiff'][key]['shape'])
                dmgr_tiff[key]['tiles'].append(dict(
                    tile=tile,
                    **cov['rangeAlternates']['dmgr:tiff'][key],
                ))
    return {
        "dmgr:tiff": dmgr_tiff
    }


