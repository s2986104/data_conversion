import os.path
import uuid

from osgeo import gdal, osr

from data_conversion.utils import transform_pixel


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
}


NAMESPACE_UUID = uuid.uuid5(uuid.NAMESPACE_DNS, 'datamanger.bccvl.org.au')


def gen_tif_metadata(tiffile, srcdir, swiftcontainer):
    """read metadata from tiffile
    """
    md = {}
    ds = gdal.Open(tiffile)
    dsmd = ds.GetMetadata()
    if 'emission_scenario' in dsmd:
        # Future Climate
        md['genre'] = 'DataGenreFC'
    else:
        # Current Climate
        md['genre'] = 'DataGenreCC'
    # put all other known global tiff metadata into md
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
    relpath = os.path.relpath(tiffile, srcdir)
    md['url'] = os.path.join(swiftcontainer, relpath)
    return md


def gen_coverage_uuid(cov, identifier):
    # generate predictable uuid
    # kind + 'id' + genre + variable names + emsc + gcm + year + month
    # if any of these identifiers change, the uuid will be different
    md = cov['bccvl:metadata']
    # determine if Data or Dataset
    # TODO: better detection of Dataset or not
    kind = 'Data' if len(cov['parameters']) == 1 else 'Dataset'
    parts = [
        kind, identifier, md['genre'],
        ''.join(sorted(cov['parameters'].keys())),
    ]
    for key in ('emsc', 'gcm', 'year', 'month'):
        parts.append(str(md.get(key, '')))

    uid = uuid.uuid5(NAMESPACE_UUID, ''.join(parts))
    return str(uid)


def gen_tif_coverage(tiffile, url):
    ds = gdal.Open(tiffile)
    return gen_cov_json(ds, url)


def gen_dataset_coverage(coverages, aggs=[]):
    return {
        "type": "Coverage",
        "domain": get_dataset_cov_domain(coverages, aggs),
        "parameters": gen_dataset_cov_parameters(coverages, aggs),
        "ranges": {},
        "rangeAlternates": gen_dataset_cov_range_alternates(coverages, aggs)
    }


def get_coverage_extent(coverage):
    # calc extent in WGS84 for x/y axes in given CRS
    axes = coverage['domain']['axes']
    src_crs = osr.SpatialReference(
        coverage['domain']['referencing'][0]['system']['wkt']
    )
    dst_crs = osr.SpatialReference()
    dst_crs.ImportFromEPSGA(4326)
    transform = osr.CoordinateTransformation(src_crs, dst_crs)

    x_size = (axes['x']['stop'] - axes['x']['start']) / (axes['x']['num'] - 1) / 2
    y_size = abs((axes['y']['stop'] - axes['y']['start']) / (axes['y']['num'] - 1) / 2)
    # we have to subtract/add half a step to get full extent
    xs = sorted([axes['x']['start'], axes['x']['stop']])
    ys = sorted([axes['y']['start'], axes['y']['stop']])
    xs = [xs[0] - x_size, xs[1] + x_size]
    ys = [ys[0] - y_size, ys[1] + y_size]
    left_bottom = transform.TransformPoint(xs[0], ys[0])
    right_top = transform.TransformPoint(xs[1], ys[1])
    return {
        # 5 decimal digits is roughly 1m
        'left': round(left_bottom[0], 5),
        'bottom': round(left_bottom[1], 5),
        'right': round(right_top[0], 5),
        'top': round(right_top[1], 5),
    }


def gen_cov_domain_axes(ds):
    # calculate raster mid points
    # axes ranges are defined as closed interval
    p0 = transform_pixel(ds, 0.5, 0.5)
    p1 = transform_pixel(ds, ds.RasterXSize - 0.5, ds.RasterYSize - 0.5)

    return {
        "x": {"start": p0[0], "stop": p1[0], "num": ds.RasterXSize},
        "y": {"start": p0[1], "stop": p1[1], "num": ds.RasterYSize},
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


def gen_cov_parameters(ds):
    # All our datasets have only one band
    band = ds.GetRasterBand(1)
    bandmd = band.GetMetadata_Dict()
    return {
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
                "unit": {  # categories don't have units
                    # TODO: maybe switch to UCUM units?
                    # There is no id for udunits
                    # "id": "",  # optional
                    # We have no label for udunits (yet)
                    # "label": {"en": ""},  # at least label or symbol is
                    #          required
                    # We have udunits, for which there is no type url,
                    #    so we use the string version of symbol
                    "symbol": bandmd['units'],  # can be a string
                    # "symbol": {    # or an object
                    #     "value": "",
                    #     "type": "",
                    # }
                }
            }
        }
    }


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


def gen_cov_json(ds, url):
    return {
        "type": "Coverage",
        "domain": gen_cov_domain(ds),
        "parameters": gen_cov_parameters(ds),
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
        # collect all values for this agg
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
    # copy all uniquely named parameters
    parameters = {}
    for coverage in coverages:
        for key, value in coverage['parameters'].items():
            if key in parameters:
                continue
            parameters[key] = value
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


