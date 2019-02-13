#!/usr/bin/env python
import argparse
import glob
import json
import os
import os.path
import uuid

from osgeo import gdal, osr
import tqdm


# TODO: add mimetype somehwere?

CATEGORY = 'climate'
RESOLUTION = '2.5 arcmin'
CURRENT_CITATION = (
    'Jones, D. A., Wang, W., & Fawcett, R. (2009). High-quality spatial '
    'climate data-sets for Australia. Australian Meteorological and '
    'Oceanographic Journal, 58(4), 233.'
)
CURRENT_TITLE = 'Australia, Current Climate (1976-2005), {resolution} (~5 km)'.format(resolution=RESOLUTION)
FUTURE_TITLE = 'Australia, Climate Projection, {resolution}'.format(resolution=RESOLUTION)
FUTURE_ACKNOWLEDGEMENT = (
    'Vanderwal, Jeremy. (2012). All future climate layers for Australia - 5km '
    'resolution. James Cook University. [Data files] '
    'jcu.edu.au/tdh/collection/633b4ccd-2e78-459d-963c-e43d3c8a5ca1'
)
FUTURE_EXTERNAL_URL = (
    'http://wallaceinitiative.org/climate_2012/tdhtools/Search/'
    'DataDownload.php?coverage=australia-5km'
)
FUTURE_LICENSE = (
    'Creative Commons Attribution 3.0 AU '
    'http://creativecommons.org/licenses/by/3.0/au'
)
SWIFT_CONTAINER = (
    'https://swift.rc.nectar.org.au/v1/AUTH_0bc40c2c2ff94a0b9404e6f960ae5677/'
    'australia_5km_layers'
)

COLLECTION = {
    "_type": "Collection",
    "uuid": "1db9e574-2f14-11e9-b0ea-0242ac110002",
    "title": "Australia current and future climate data",
    "description": "Current and future climate data for the Australian continent\n\nGeographic extent: Australia\nYear range: 1976-2005, 2015-2085\nResolution: 30 arcsec (~1 km), 2.5 arcmin (~5 km)\nData layers: B01-19",
    "rights": "CC-BY Attribution 3.0",
    "landingPage": "See <a href=\"https://research.jcu.edu.au/researchdata/default/detail/a06a78f553e1452bcf007231f6204f04/\">https://research.jcu.edu.au/researchdata/default/detail/a06a78f553e1452bcf007231f6204f04/</a>",
    "attribution": ["Vanderwal, Jeremy. (2012). All future climate layers for Australia - 5km resolution. James Cook University."],
    "subjects": ["Current datasets", "Future datasets"],
    "categories": ["climate"],
    "datasets": [
        # {
        #     "uuid": "8f6e3ea5-1caf-5562-a580-aa23bbe7c975",
        #     "title": "Australia, Current Climate (1976-2005), 2.5 arcmin (~5 km)"
        # },
        # {
        #     "uuid": "fecc0b23-4199-5b49-ac6c-45c3c1249f3e",
        #     "title": "Australia, Climate Projection, 2.5 arcmin"
        # }
    ],

    "BCCDataGenre": ["DataGenreCC", "DataGenreFC"]
}


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

NAMESPACE_UUID = uuid.uuid5(uuid.NAMESPACE_DNS, 'datamanger.bccvl.org.au')


def gen_uuid(cov):
    # generate predictable uuid
    # kind + 'id' + genre + varaible names
    # kind + 'id' + genre + variable names + emsc + gcm + year
    # if any of these identifiers change, the uuid will be different
    md = cov['bccvl:metadata']
    # determine if Data or Dataset
    kind = 'Data' if len(cov['parameters']) == 1 else 'Dataset'
    if md['genre'] == 'DataGenreCC' or kind == 'Dataset':
        # it's a current coverage
        uid = uuid.uuid5(
            NAMESPACE_UUID,
            ''.join([
                kind, 'australia-5km', md['genre'],
                ''.join(sorted(cov['parameters'].keys()))
            ])
        )
    else:
        # it's a future coverage
        uid = uuid.uuid5(
            NAMESPACE_UUID,
            ''.join([
                kind, 'australia-5km', md['genre'],
                ''.join(sorted(cov['parameters'].keys())),
                md['emsc'], md['gcm'], str(md['year'])
            ])
        )
    return str(uid)


# convert pixel to projection unit
def transform_pixel(ds, x, y):
    xoff, a, b, yoff, d, e = ds.GetGeoTransform()
    return (
        # round lat/lon to 5 digits which is about 1cm at equator
        round(a * x + b * y + xoff, 5),
        round(d * x + e * y + yoff, 5),
    )


def get_cov_domain_axes(ds):
    # calculate raster mid points
    # axes ranges are defined as closed interval
    p0 = transform_pixel(ds, 0.5, 0.5)
    p1 = transform_pixel(ds, ds.RasterXSize - 0.5, ds.RasterYSize - 0.5)

    return {
        "x": {"start": p0[0], "stop": p1[0], "num": ds.RasterXSize},
        "y": {"start": p0[1], "stop": p1[1], "num": ds.RasterYSize},
        # tiffs are anly 2 dimensional, so no need to add any further axes
    }


def get_cov_referencing(ds):
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


def get_cov_domain(ds):
    return {
        "type": "Domain",
        "domainType": "Grid",
        "axes": get_cov_domain_axes(ds),
        "referencing": get_cov_referencing(ds),
    }


def get_cov_parameters(ds):
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


def get_cov_ranges(ds):
    # it's binary tiff, potentially very large,
    # so our ranges are empty
    return {}


def get_cov_range_alternates(ds, url):
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


def get_cov_json(ds, url):
    return {
        "type": "Coverage",
        "domain": get_cov_domain(ds),
        "parameters": get_cov_parameters(ds),
        "ranges": get_cov_ranges(ds),
        "rangeAlternates": get_cov_range_alternates(ds, url)
    }


def gen_coverage(tiffile, url):
    ds = gdal.Open(tiffile)
    return get_cov_json(ds, url)


def get_dataset_cov_domain_axes(coverages, aggs):
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
            bounds = {tuple(c['bccvl:metadata']['year_range']) for c in coverages}
            axes[agg]['bounds'] = [item for sublist in bounds for item in sublist]
    return axes


def get_dataset_cov_referencing(coverages, aggs):
    # just copy from first coverage, we don't add any projected axes
    return coverages[0]['domain']['referencing']


def get_dataset_cov_domain(coverages, aggs):
    return {
        "type": "Domain",
        "domainType": "Grid",
        "axes": get_dataset_cov_domain_axes(coverages, aggs),
        "referencing": get_dataset_cov_referencing(coverages, aggs),
    }


def get_dataset_cov_parameters(coverages, aggs):
    # copy all uniquely named parameters
    parameters = {}
    for coverage in coverages:
        for key, value in coverage['parameters'].items():
            if key in parameters:
                continue
            parameters[key] = value
    return parameters


def get_dataset_cov_range_alternates(coverages, aggs):
    # build dmgr:tiff dict
    dmgr_tiff = {}
    if len(aggs) == 0:
        for cov in coverages:
            # one range for each variable/parameter
            for key in cov['rangeAlternates']['dmgr:tiff'].keys():
                dmgr_tiff[key] = cov['rangeAlternates']['dmgr:tiff'][key]
    else:
        # TODO: we should not re-calc this to get the shape for aggregated axes
        axes = get_dataset_cov_domain_axes(coverages, aggs)
        aggs_shape = [len(axes[x]['values']) for x in aggs]
        for cov in coverages:
            # check if variable is already defined
            for key in cov['rangeAlternates']['dmgr:tiff'].keys():
                if key not in dmgr_tiff:
                    # create variable
                    dmgr_tiff[key] = {
                        "type": "dmgr:TIFF2DAggregation",
                        "datatype": cov['rangeAlternates']['dmgr:tiff'][key]['datatype'],
                        "axisNames": aggs + cov['rangeAlternates']['dmgr:tiff'][key]['axisNames'],
                        "shape": aggs_shape + cov['rangeAlternates']['dmgr:tiff'][key]['shape'],
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


def gen_dataset_coverage(coverages, aggs=[]):
    return {
        "type": "Coverage",
        "domain": get_dataset_cov_domain(coverages, aggs),
        "parameters": get_dataset_cov_parameters(coverages, aggs),
        "ranges": {},
        "rangeAlternates": get_dataset_cov_range_alternates(coverages, aggs)
    }


def gen_coverage_metadata(tifffile, swiftcontainer):
    """read metadata template and populate rest of fields
    and write to metadata.json'
    """
    md = {}
    ds = gdal.Open(tifffile)
    dsmd = ds.GetMetadata()
    years = dsmd['year_range'].split('-')
    if 'emission_scenario' in dsmd:
        # Future Climate
        md['genre'] = 'DataGenreFC'
        # TODO: we have tied the attribute name to the axis name ...
        #       maybe we should rethink how we name axes, and potentially give
        #       them different names than the attribute
        md['emsc'] = dsmd['emission_scenario']
        md['gcm'] = dsmd['general_circulation_models']
        md['year'] = int(dsmd['year'])
        md['year_range'] = [int(years[0]), int(years[1])]
        # TODO: external url?, acknowledgement?
    else:
        # Current Climate
        md['genre'] = 'DataGenreCC'
        md['year'] = int(dsmd['year'])
        md['year_range'] = [int(years[0]), int(years[1])]
        md['acknowledgement'] = CURRENT_CITATION
        md['external_url'] = ''
    md['resolution'] = RESOLUTION

    # build swift path:
    base = os.path.basename(os.path.dirname(tifffile))
    filename = os.path.basename(tifffile)
    md['url'] = os.path.join(swiftcontainer, base, filename)
    return md


def gen_dataset_metadata(genre):
    ds_md = {
        'category': CATEGORY,
        'genre': genre,
        'resolution': RESOLUTION,
        'acknowledgement': FUTURE_ACKNOWLEDGEMENT,
        'external_url': FUTURE_EXTERNAL_URL,
        'license': FUTURE_LICENSE,

    }
    # ds_md[u'bounding_box'] = md[u'bounding_box']
    # ds_md[u'layers'] = [ lyr['url'] for lyr in layermds if lyr['genre'] == genre ]
    if genre == 'DataGenreFC':
        ds_md['title'] = FUTURE_TITLE
    else:
        ds_md['title'] = CURRENT_TITLE
    return ds_md


def get_extent(coverage):
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
        'left': left_bottom[0],
        'bottom': left_bottom[1],
        'right': right_top[0],
        'top': right_top[1],
    }


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true',
                        help='Re generate data.json form tif files.')
    parser.add_argument('srcdir')
    return parser.parse_args()


def main():
    # TODO: we need a mode to just update as existing json file without parsing
    #       all tiff files. This would be useful to just update titles and
    #       things.
    #       could probably also be done in a separate one off script?
    opts = parse_args()
    opts.srcdir = os.path.abspath(opts.srcdir)

    datajson = os.path.join(opts.srcdir, 'data.json')
    print("Generate data.json")
    if not os.path.exists(datajson) or opts.force:
        print("Rebuild data.json")
        # rebuild data.json
        coverages = []
        # generate all coverages inside source folder
        for tiffile in tqdm.tqdm(sorted(glob.glob(os.path.join(opts.srcdir, '*/*')))):
            try:
                md = gen_coverage_metadata(tiffile, SWIFT_CONTAINER)
                coverage = gen_coverage(tiffile, md['url'])
                coverage['bccvl:metadata'] = md
                coverage['bccvl:metadata']['extent_wgs84'] = get_extent(coverage)
                coverage['bccvl:metadata']['uuid'] = gen_uuid(coverage)
                coverages.append(coverage)
            except Exception as e:
                print('Failed to generate metadata for:', tiffile, e)
                raise

        print("Write data.json")
        with open(datajson, 'w') as mdfile:
            json.dump(coverages, mdfile, indent=2)
    else:
        print("Use existing data.json")
        coverages = json.load(open(datajson))

    print("Generate datasets.json")
    datasets = []
    # generate datasets for db import
    for genre in ("DataGenreCC", "DataGenreFC"):
        # filter coverages by genre and build coverage aggregation over all
        # remaining coverages
        subset = list(filter(
            lambda x: x['bccvl:metadata']['genre'] == genre,
            coverages)
        )
        aggs = [] if genre == 'DataGenreCC' else ['emsc', 'gcm', 'year']
        coverage = gen_dataset_coverage(subset, aggs)
        md = gen_dataset_metadata(genre)
        coverage['bccvl:metadata'] = md
        coverage['bccvl:metadata']['extent_wgs84'] = get_extent(coverage)
        coverage['bccvl:metadata']['uuid'] = gen_uuid(coverage)
        datasets.append(coverage)

    print("Write datasets.json")
    # save all the data
    with open(os.path.join(opts.srcdir, 'datasets.json'), 'w') as mdfile:
        json.dump(datasets, mdfile, indent=2)

    print("Write collection.json")
    with open(os.path.join(opts.srcdir, 'collection.json'), 'w') as mdfile:
        # add datasets
        for ds in datasets:
            COLLECTION['datasets'].append({
                "uuid": ds['bccvl:metadata']['uuid'],
                "title": ds['bccvl:metadata']['title']
            })
        json.dump([COLLECTION], mdfile, indent=2)


if __name__ == "__main__":
    main()
