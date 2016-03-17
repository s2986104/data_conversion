import numpy as np
import scipy.stats
from osgeo import gdal
import glob
import os
import os.path
import subprocess
import json
import sys
import re
import shutil
from collections import defaultdict
import logging

JSON_TEMPLATE = 'fpar.stats.template.json'


def initialise_logger():
    """Initialise python logger module. This is the primary
    output of the script.

    Keyword arugments: None

    Returns: Initialised logger.
    """

    # TODO: Exception handling
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    form = "%(levelname)8s %(asctime)s %(funcName)s %(lineno)d %(message)s"
    formatter = logging.Formatter(form)

    # STDOUT logging
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.info("Logger initialised ok")
    return logger


def write_array_to_raster(outfile, dataset, template):
    """Write numpy array to raster (geoTIFF format).

    Keyword arguments:
    outfile -- name of the output file
    dataset -- numpy array to be written to file
    template -- path to a gdal dataset to use as template

    Returns: None.
    """
    log.info("Writing to {}".format(outfile))

    # open template dataset
    templateds = gdal.Open(template)

    # create new dataset
    outdata = templateds.GetDriver().CreateCopy(outfile, templateds, options=("COMPRESS=LZW", "TILED=YES"))

    # write data to file
    outdata.GetRasterBand(1).WriteArray(dataset)

    # calculate statistics
    outdata.GetRasterBand(1).ComputeStatistics(False)

    # flush data to disk
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
        calyearlist[year] = glob.glob(os.path.join(fparroot, 'fpar.{0}.*.aust.tif'.format(year)))
        for month in range(01, 13, 1):
            fname = os.path.join(fparroot.replace("*", 'fpar.{0}.{1:02d}.aust'.format(year, month)), 'fpar.{0}.{1:02d}.aust.tif'.format(year, month))
            if os.path.isfile(fname):
                if month < 7:
                    growyearlist[year-1].append(fname)
                else:
                    growyearlist[year].append(fname)

    # Create the lists - monthly
    monthlylist = defaultdict(list)
    for month in range(01, 13, 1):
        monthlylist["{0:02d}".format(month)] = glob.glob(os.path.join(fparroot, 'fpar.*.{0:02d}.aust.tif'.format(month)))

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

    # Define statistics related arrays
    meanarr = np.zeros((rows, cols))
    minarr = np.zeros((rows, cols))
    maxarr = np.zeros((rows, cols))
    # covarr = np.zeros((rows, cols))

    # Define chunk size
    xBSize = 1024 * 4
    yBSize = 1024 * 4

    # Reads rasters in in chunks to minimise memory load
    for y in range(0, rows, yBSize):
        if y + yBSize < rows:
            numRows = yBSize
        else:
            numRows = rows - y

        for x in range(0, cols, xBSize):
            if x + xBSize < cols:
                numCols = xBSize
            else:
                numCols = cols - x

            log.info("Processing row {} block {} col {} block {}".format(y, numRows, x, numCols))
            # Create a rasterStack with chunks from datasources
            stackRast = np.dstack(
                [np.array(ds.GetRasterBand(1).ReadAsArray(x, y, numCols, numRows)) for ds in dss]
            )

            log.debug("-- Calculating stats")
            try:
                meanarr[y:y + numRows, x:x + numCols] = np.mean(stackRast, axis=2)
                minarr[y:y + numRows, x:x + numCols] = np.min(stackRast, axis=2)
                maxarr[y:y + numRows, x:x + numCols] = np.max(stackRast, axis=2)
                # covarr[y:y+numRows, x:x+numCols] = scipy.stats.variation(stackRast, axis=2)
            except Exception:
                log.exception("Error calculating statistics...")
     # Construct a dictionary as result
    return {
        'mean': meanarr,
        'min': minarr,
        'max': maxarr,
        # 'cv': covarr
    }


def build_dataset(stats, template, destroot, workdir, fnameformat, year=0, month=0):
    """Load in raster files in chunks to reduce memory demands,
    calculate statistics, and save to file.

    Keyword arguments:
    stats -- dictionary with numpy arrayrs to store as datasets
    template -- a gdal dataset to be used as template to create new ones
    destfolder -- the folder to store the final zip file in
    tmpdir -- some scratch location with enough working disk space
    fnameformat --  flag that determines formatting
    year -- optional arugment used to supply year for formatting
    month -- optional argument used to supply month for formatting

    Returns: None.
    """
    # generate new file name
    if fnameformat == 'global':
        descriptor = "2000-2014"
    elif fnameformat == 'growyearly':
        descriptor = "{:04d}-{:04d}".format(year, year + 1)
    elif fnameformat == 'calyearly':
        descriptor = "{:04d}".format(year)
    elif fnameformat == 'monthly':
        descriptor = month
    # create zip root
    ziproot = os.path.join(workdir, "fpar.{0}.stats.aust".format(descriptor))
    check_or_create_target_dir(ziproot)

    # Write the results to raster format with appropriate filenames
    for stattype, statarr in stats.items():
        outfile = os.path.join(ziproot, 'data', "fpar.{0}.{1}.aust.tif".format(descriptor, stattype))
        write_array_to_raster(outfile, statarr, template)

    # Write the metadata.json file
    write_metadatadotjson(ziproot, fnameformat, year, month)
    # Zip up the dataset
    zip_dataset(ziproot, destroot)

    # Clean up the directories
    shutil.rmtree(ziproot)


def write_metadatadotjson(ziproot, fnameformat, year=0, month=0):
    """Write BCCVL metadata to json file.

    Keyword arguments:
    dest -- destination for metadata file
    fnameformat --  flag that determines formatting
    year -- optional arugment used to supply year for formatting
    month -- optional argument used to supply month for formatting

    Returns: None.
    """
    if fnameformat == 'global':
        title = "2000 to 2014 (Average, Minimum, Maximum)"
        rexp = r'fpar\.(.{9})\.(mean|max|min|cv)\.*'
    elif fnameformat == 'growyearly':
        title = "{:04d} to {:04d} Growing Year (Average, Minumum, Maximum)".format(year, year + 1)
        rexp = r'fpar\.(.{9})\.(mean|max|min|cv)\.*'
    elif fnameformat == 'calyearly':
        title = "{:04d} Calendar Year (Average, Minumum, Maximum)".format(year)
        rexp = r'fpar\.(.{4})\.(mean|max|min|cv)\.*'
    elif fnameformat == 'monthly':
        title = "{} (Long-term Monthly Average, Minimum, Maximum)".format(month)
        rexp = r'fpar\.(.{2})\.(mean|max|min|cv)\.*'

    log.info("{} {}".format(fnameformat, title))

    workdir = os.path.dirname(ziproot.rstrip(os.path.sep))

    md = json.load(open(JSON_TEMPLATE, 'r'))
    md[u'files'] = {}
    for filename in glob.glob(os.path.join(ziproot, '*', '*.tif')):
        base = os.path.basename(filename)
        m = re.match(rexp, base)
        stattype = m.group(2)
        layer_id = 'FPAR{}'.format(stattype)
        md[u'title'] = md[u'title'].format(title=title)
        filename = os.path.relpath(filename, workdir)
        md[u'files'][filename] = {
            u'layer': layer_id,
        }

    mdfile = open(os.path.join(ziproot, 'bccvl', 'metadata.json'), 'w')
    json.dump(md, mdfile, indent=4)
    mdfile.close()


def zip_dataset(ziproot, dest):
    """Zip a folder to specified destination, with .DS_Store exclusion.

    Keyword arguments:
    ziproot -- location of zip target
    dest -- destination for zipfile

    Returns: None.
    """
    workdir = os.path.dirname(ziproot)
    zipdir = os.path.basename(ziproot)
    zipname = os.path.abspath(os.path.join(dest, zipdir + '.zip'))

    ret = os.system(
        'cd {0}; zip -r {1} {2} -x *.aux.xml* -x *.DS_Store'.format(workdir, zipname, zipdir)
    )
    if ret != 0:
        raise Exception("can't zip {0} ({1})".format(ziproot, ret))


def check_or_create_target_dir(target):
    """Check if target directory exists, if not recursively create

    Keyword arguments:
    target -- directory to be checked

    Returns: None.
    """
    if not os.path.isdir(target):
        try:
            os.mkdir(target)
            os.mkdir(os.path.join(target, 'data'))
            os.mkdir(os.path.join(target, 'bccvl'))
        except:
            log.exception("Unable to create target {}".format(target))

def fpar_stats(destroot, workdir, tif_dir='tifs'):
        # Generate the lists for global, long-term monthly, and yearly raster stacks
        (glbl, mntly, growyrly, calyrly) = get_file_lists(tif_dir)

        # Calculate the statistics
        for mth in mntly.keys():
            stats = raster_chunking_stats(mntly[mth])
            build_dataset(stats, mntly[mth][0], destroot, workdir, 'monthly', month=mth)
        for yr in growyrly.keys():
            stats = raster_chunking_stats(growyrly[yr])
            build_dataset(stats, growyrly[yr][0], destroot, workdir, 'growyearly', year=yr)
        for yr in calyrly.keys():
            stats = raster_chunking_stats(calyrly[yr])
            build_dataset(stats, calyrly[yr][0], destroot, workdir, 'calyearly', year=yr)
        stats = raster_chunking_stats(glbl)
        build_dataset(stats, glbl[0], destroot, workdir, 'global', year='2000-2014')


log = initialise_logger()
log.info("-----------------------------------------------------------")
log.info("[BEGIN EXECUTION]")
