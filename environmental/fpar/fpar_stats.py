import numpy as np
from scipy import stats
from osgeo import gdal, gdal_array
import glob
import os
import os.path
from collections import defaultdict
import tempfile
from tqdm import trange


def write_array_to_raster(outfile, dataset, template):
    """Write numpy array to raster (geoTIFF format).

    Keyword arguments:
    outfile -- name of the output file
    dataset -- numpy array to be written to file
    template -- path to a gdal dataset to use as template

    Returns: None.
    """

    # open template dataset
    templateds = gdal.Open(template)

    outdata = gdal.GetDriverByName('GTiff').Create(
        outfile,
        xsize=dataset.shape[1],  # X
        ysize=dataset.shape[0],  # Y
        bands=1,
        eType=gdal_array.NumericTypeCodeToGDALTypeCode(dataset.dtype),
        # uncompressed data is quicker to write, but usually takes up 3 times the space
        options=("COMPRESS=DEFLATE", "TILED=YES")
    )
    # copy infos from templateds
    gdal_array.CopyDatasetInfo(templateds, outdata)

    band = outdata.GetRasterBand(1)
    # preserve no data value
    # TODO: this only works because we pass NoDataValue through everywhere
    #       otherwise we should set a proper nodatavalue here and
    #       replace all nan's in dataset to this value before writing
    band.SetNoDataValue(templateds.GetRasterBand(1).GetNoDataValue())
    # write data
    band.WriteArray(dataset)
    # no stats compute required, we do that later anyway
    outdata.FlushCache()



def get_file_lists(fparroot):
    """Construct file lists (dictionary data type) for global, long term-monthly, growing years, and calendar years.

    Keyword arguments:
    fparroot -- directory containing source geoTIFF files

    Returns:
    globallist -- a list of all geoTIFF files in fparroot
    monthlylist -- a dictionary with month numbers as keys, matching files as values.
    growyearlist -- a dictionary with year numbers (first year of growing year) as keys, matching files as values.
    calyearlist -- a dictionary with calendar year numbers as keys, matching files as values.
    """

    # Fetch all of the fpar files in this dir
    globallist = glob.glob(os.path.join(fparroot, 'fpar.*.*.aust.tif'))

    # Create the lists - yearly
    calyearlist = defaultdict(list)
    growyearlist = defaultdict(list)
    for year in range(2000, 2015, 1):

        file_list = glob.glob(os.path.join(fparroot, 'fpar.{0}.*.aust.tif'.format(year)))
        if len(file_list) > 0:
            calyearlist[year] = file_list
        for month in range(1, 13, 1):
            fname = os.path.join(fparroot.replace("*", 'fpar.{0}.{1:02d}.aust'.format(year, month)), 'fpar.{0}.{1:02d}.aust.tif'.format(year, month))
            if os.path.isfile(fname):
                if month < 7:
                    growyearlist[year-1].append(fname)
                else:
                    growyearlist[year].append(fname)

    # Create the lists - monthly
    monthlylist = defaultdict(list)
    for month in range(1, 13, 1):
        file_list = glob.glob(os.path.join(fparroot, 'fpar.*.{0:02d}.aust.tif'.format(month)))
        if len(file_list) > 0:
            monthlylist["{0:02d}".format(month)] = file_list

    return globallist, monthlylist, growyearlist, calyearlist


def raster_chunking_stats(imlist):
    """Load in raster files in chunks to reduce memory demands,
    calculate statistics, and save to file.

    Keyword arguments:
    imlist -- a list of filenames

    Returns: None.
    """

    if len(imlist) == 0:
        return
    # Open all raster files
    dss = [gdal.Open(img) for img in imlist]
    # get size from first raster (assume all are the same size)
    rows = dss[0].GetRasterBand(1).YSize
    cols = dss[0].GetRasterBand(1).XSize
    dtype = gdal_array.GDALTypeCodeToNumericTypeCode(dss[0].GetRasterBand(1).DataType)
    nodata = dtype(dss[0].GetRasterBand(1).GetNoDataValue())

    # Define statistics related arrays
    meanarr = np.memmap(filename=tempfile.NamedTemporaryFile(prefix='mean_'),
                        dtype=dtype, shape=(rows, cols))
    minarr = np.memmap(filename=tempfile.NamedTemporaryFile(prefix='min_'),
                       dtype=dtype, shape=(rows, cols))
    maxarr = np.memmap(filename=tempfile.NamedTemporaryFile(prefix='max_'),
                       dtype=dtype, shape=(rows, cols))
    # covarr = np.zeros((rows, cols))

    # Define chunk size
    xBSize, yBSize = dss[0].GetRasterBand(1).GetBlockSize()
    xBSize = xBSize * 4
    yBSize = yBSize * 4

    # Reads rasters in in chunks to minimise memory load
    for y in trange(0, rows, yBSize, desc='calc min/max/mean blocks'):
        if y + yBSize < rows:
            numRows = yBSize
        else:
            numRows = rows - y

        for x in range(0, cols, xBSize):
            if x + xBSize < cols:
                numCols = xBSize
            else:
                numCols = cols - x

            # Create a rasterStack with chunks from datasources
            stackRast = np.stack((np.array(ds.GetRasterBand(1).ReadAsArray(x, y, numCols, numRows)) for ds in dss))
            # replace nodata with nan; we do this to avoid considering nodata values for statistics
            # TODO: should we use nanmean/nanmin/nanmax?
            #       these functions wolud return values if only some values are nan, whereas
            #       the normal methods return nan if any value is nan
            stackRast[stackRast == nodata] = np.nan
            try:
                meanarr[y:y + numRows, x:x + numCols] = np.mean(stackRast, axis=0)
                minarr[y:y + numRows, x:x + numCols] = np.min(stackRast, axis=0)
                maxarr[y:y + numRows, x:x + numCols] = np.max(stackRast, axis=0)
            except Exception as e:
                print("Error calculating statistics...", e)
                raise
    # replace all nan's with nodata (we don't want to write nan's to the resulting tiff)
    meanarr[np.isnan(meanarr)] = nodata
    minarr[np.isnan(minarr)] = nodata
    maxarr[np.isnan(maxarr)] = nodata
    # Construct a dictionary as result
    return {
        'mean': meanarr,
        'min': minarr,
        'max': maxarr,
        # 'cov': covarr
    }


def calc_cov(dsfiles):
    """Calculates CoV over given list of input files

    dsfiles ... list of files to calculate CoV from
    returns numpy array
    """

    # open files
    datasets = [gdal.Open(fname) for fname in dsfiles]
    # check shape of all datasets:
    shape = set((ds.RasterYSize, ds.RasterXSize) for ds in datasets)
    if len(shape) != 1:
        raise Exception("Raster have different shape")
    ysize, xsize = shape.pop()
    # determine dtype and nodata
    dtype = gdal_array.GDALTypeCodeToNumericTypeCode(dss[0].GetRasterBand(1).DataType)
    nodata = dtype(dss[0].GetRasterBand(1).GetNoDataValue())

    result = np.memmap(filename=tempfile.NamedTemporaryFile(prefix='cov_'),
                       dtype=dtype, shape=(ysize, xsize))
    # build buffer array for blocked reading (assume same block size for all datasets, and only one band)
    x_block_size, y_block_size = datasets[0].GetRasterBand(1).GetBlockSize()
    x_block_size = x_block_size * 4
    y_block_size = y_block_size * 4
    for i in trange(0, ysize, y_block_size, desc='calc cov blocks'):
        # determine block height to read
        if i + y_block_size < ysize:
            rows = y_block_size
        else:
            rows = ysize - i
        # determine blogk width to read
        for j in range(0, xsize, x_block_size):
            if j + x_block_size < xsize:
                cols = x_block_size
            else:
                cols = xsize - j

            #print("Processing row {} block {} col {} block {}".format(i, rows, j, cols))
            inarr = np.dstack((
                ds.GetRasterBand(1).ReadAsArray(xoff=j, yoff=i,
                                                win_xsize=cols, win_ysize=rows)
                for ds in datasets))
            # replace nodata with nan
            inarr[inarr == nodata] = np.nan
            # calculate coefficient of variance across datasets (axis 2)
            result[i:i+inarr.shape[0], j:j+inarr.shape[1]] = stats.variation(inarr, axis=2)

    # replace all nan's with nodata
    result[result.isnan()] = nodata
    # ???
    result[result>1.0] = nodata
    return result

def stat_filename(destdir, md):
    filename = "{0}_{1}_{2}".format(os.path.basename(destdir), md['fnameformat'], md['layerid'])
    if md['fnameformat'] == 'monthly':
        filename += "_{:02d}.tif".format(md['month'])
    else:
        filename += "_{:04d}.tif".format(md['year'])
    return os.path.join(destdir, filename)
