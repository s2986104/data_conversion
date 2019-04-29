import argparse
from concurrent import futures
import copy
import glob
import json
import os.path
import shutil
import tempfile
import zipfile

from osgeo import gdal
from tqdm import tqdm

from data_conversion.coverage import (
    gen_tif_metadata,
    gen_tif_coverage,
    get_coverage_extent,
    gen_coverage_uuid,
    gen_dataset_coverage,
)
from data_conversion.utils import (
    ensure_directory,
    get_vsi_path,
    match_coverage,
    move_files,
    retry_run_cmd,
    product_dict,
)
from data_conversion.vocabs import (
    VAR_DEFS,
    PREDICTORS,
)


# TODO: set scale and offset in md
def run_gdal(cmd, infile, outfile, md):
    """
    Run gdal_translate in sub process.
    """
    _, tfname = tempfile.mkstemp(suffix='.tif')
    try:
        retry_run_cmd(cmd + [infile, tfname])
        # add band metadata
        # this is our temporary geo tiff, we should be able to open that
        # without problems
        ds = gdal.Open(tfname, gdal.GA_Update)
        band = ds.GetRasterBand(1)
        # ensure band stats
        band.ComputeStatistics(False)
        layerid = md['layerid']
        for key, value in VAR_DEFS[layerid].items():
            band.SetMetadataItem(key, value)
        # just for completeness
        band.SetUnitType(VAR_DEFS[layerid]['units'])
        band.SetScale(md.get('scale', 1.0))
        band.SetOffset(md.get('offset', 0.0))
        ds.FlushCache()
        # build command
        cmd = [
            'gdal_translate',
            '-of', 'GTiff',
            '-co', 'TILED=yes',
            '-co', 'COPY_SRC_OVERVIEWS=YES',
            '-co', 'COMPRESS=DEFLATE',
            '-co', 'PREDICTOR={}'.format(PREDICTORS[band.DataType]),
        ]
        # check rs
        if not ds.GetProjection():
            cmd.extend(['-a_srs', 'EPSG:4326'])
        # close dataset
        del band
        del ds
        # gdal_translate once more to cloud optimise geotiff
        cmd.extend([tfname, outfile])
        retry_run_cmd(cmd)
    except Exception as e:
        print('Error:', e)
        raise e
    finally:
        os.remove(tfname)



class BaseConverter(object):

    # scale and offset per layerid
    SCALES = {}
    OFFSETS = {}

    def __init__(self):
        super().__init__()

    # helper mehods to override in sub classes
    def parse_zip_filename(self, srcfile):
        """
        parse an absolute path to a zip file and return a dict
        with information extracted from full path and filename

        common keys are
        layerid, year, emsc, gcm, rcm, ...
        """
        return {}

    def parse_filename(self, filename):
        """
        parse a full path within a zip file and return a dict
        with informatn extracted from path and filename
        """
        return {}

    def target_dir(self, destdir, srcfile):
        """
        return an absolute path to be used as target directory within destdir.
        This method usually uses parse_xxx methods to get information
        about srcfile and construct a subfolder name / hierarchy from
        that information.
        """
        raise NotImplementedError()

    def gdal_options(self, md):
        """
        Create gdal cmd line from metadata dictionary
        """
        raise NotImplementedError()

    def skip_zipinfo(self, zipinfo):
        """
        return true to ignore this zipinfo entry
        """
        # default ignore directories
        if zipinfo.is_dir():
            return True
        # ignore none .tif, .asc files
        _, ext = os.path.splitext(zipinfo.filename)
        if ext not in ('.tif', '.asc'):
            return True
        return False

    def destfilename(self, destdir, md):
        """
        generate file name for output tif file.
        """
        return (
            os.path.basename(destdir) +
            '_' +
            md['layerid'].replace('_', '-') +
            '.tif'
        )

    def filter_srcfiles(self, srcfile):
        """
        return False to skip this srcfile (zip file)
        """
        return True

    # common methods usually not required to change
    def create_target_dir(self, destdir, srcfile, check=False):
        root = self.target_dir(destdir, srcfile)
        if check:
            return os.path.exists(root)
        else:
            os.makedirs(root, exist_ok=True)
        return root

    def convert(self, srcfile, destdir):
        """convert .asc.gz files in folder to .tif in dest
        """
        parsed_zip_md = self.parse_zip_filename(srcfile)
        pool = futures.ProcessPoolExecutor(3)
        results = []
        with zipfile.ZipFile(srcfile) as srczip:
            for zipinfo in tqdm(srczip.filelist, desc="build jobs"):
                if self.skip_zipinfo(zipinfo):
                    continue

                parsed_md = copy.copy(parsed_zip_md)
                parsed_md.update(
                    self.parse_filename(zipinfo.filename)
                )
                # apply scale and offset
                if parsed_md['layerid'] in self.SCALES:
                    parsed_md['scale'] = self.SCALES[parsed_md['layerid']]
                if parsed_md['layerid'] in self.OFFSETS:
                    parsed_md['offset'] = self.OFFSETS[parsed_md['layerid']]
                destfilename = self.destfilename(destdir, parsed_md)
                srcurl = get_vsi_path(srcfile, zipinfo.filename)
                gdaloptions = self.gdal_options(parsed_md)
                # output file name
                destpath = os.path.join(destdir, destfilename)
                # run gdal translate
                cmd = ['gdal_translate']
                cmd.extend(gdaloptions)
                results.append(
                    pool.submit(run_gdal, cmd, srcurl, destpath, parsed_md)
                )

        for result in tqdm(futures.as_completed(results),
                                desc=os.path.basename(srcfile),
                                total=len(results)):
            if result.exception():
                tqdm.write("Job failed")
                raise result.exception()

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            'source', action='store',
            help='source folder or source zip file.'
        )
        parser.add_argument(
            'destdir', action='store',
            help='destination folder for converted tif files.'
        )
        parser.add_argument(
            '--workdir', action='store',
            default='/mnt/workdir/australia_work',
            help=('folder to store working files before moving to final '
                  'destination')
        )
        parser.add_argument(
            '--skipexisting', action='store_true',
            help='Skip files for which destination dir exists. (no checks done)'
        )
        return parser.parse_args()

    def main(self):
        """
        start the conversion process
        """
        opts = self.parse_args()
        srcfile = os.path.abspath(opts.source)
        if os.path.isdir(srcfile):
            srcfiles = sorted(glob.glob(os.path.join(srcfile, '**', '*.zip'), recursive=True))
        else:
            srcfiles = [srcfile]
        # Optional filter for source files
        srcfiles = list(filter(self.filter_srcfiles, srcfiles))

        workdir = ensure_directory(opts.workdir)
        dest = ensure_directory(opts.destdir)
        # unpack contains one destination datasets
        for srcfile in tqdm(srcfiles):
            target_work_dir = self.create_target_dir(workdir, srcfile)
            try:
                # TODO: skip not done anhywhere else
                if opts.skipexisting and self.create_target_dir(dest, srcfile, check=True):
                    tqdm.write('Skip {}'.format(srcfile))
                    continue
                # convert files into workdir
                self.convert(srcfile, target_work_dir)
                # move results to dest
                target_dir = self.create_target_dir(dest, srcfile)
                move_files(target_work_dir, target_dir)
            finally:
                # cleanup
                shutil.rmtree(target_work_dir)


class BaseLayerMetadata(object):

    # Dataset id/name for uuid namespace (may need parameter like resolution / version)
    CATEGORIES = None
    SWIFT_CONTAINER = None
    DATASETS = []
    DATASET_ID = ''

    def parse_filename(self, tiffile):
        """
        absolute path to tiffile, to allow info extract from full path.
        """
        return {}

    def gen_dataset_metadata(self, dsdef, coverages):
        """
        build dataset metadata from dsdef and list of data coverages
        """
        return {}

    def get_genre(self, md):
        """
        Determine genre based on metadata from tiffile.
        """
        if 'emsc' in md and 'gcm' in md:
            # Future Climate
            return 'DataGenreFC'
        else:
            # Current Climate
            return 'DataGenreCC'

    def get_rat_map(self, tiffile):
        """
        return a mapping for categories 'id', 'label', 'value' from rat column
        """
        return None

    def cov_uuid(self, dscov):
        """
        Generate data/dataset uuid for dataset coverage
        """
        return gen_coverage_uuid(dscov, self.DATASET_ID)

    def build_data(self, opts):
        coverages = []
        # generate all coverages inside source folder
        tiffiles = sorted(
            glob.glob(os.path.join(opts.srcdir, '**/*.tif'),
                      recursive=True)
        )
        for tiffile in tqdm(tiffiles):
            try:
                # TODO: fetch data stats from tiff if available
                # TODO: maybe re-order things here...
                #       0. keep oerder metadata -> coverage (md could be used to improve coverage?)
                #       1. gen md['url'] in separate step (no passing around of self.SWIFT_CONTAINER, and md['url'])
                #       2. move decision about genre into method (maybe after gen coverage?)
                md = gen_tif_metadata(tiffile, opts.srcdir, self.SWIFT_CONTAINER)
                md.update(self.parse_filename(tiffile))
                md['genre'] = self.get_genre(md)
                # TODO: move this up and pass on full md
                coverage = gen_tif_coverage(tiffile, md['url'], ratmap=self.get_rat_map(tiffile))
                md['extent_wgs84'] = get_coverage_extent(coverage)
                # TODO: the way we set acknowloedgement is weird here
                if md['genre'] == 'DataGenreCC':
                    md['acknowledgement'] = self.DATASETS[0]['acknowledgement']
                # ony set md keys without leading '_'
                coverage['bccvl:metadata'] = {key: val for (key,val) in md.items() if not key.startswith('_')}
                coverage['bccvl:metadata']['uuid'] = self.cov_uuid(coverage)
                coverages.append(coverage)
            except Exception as e:
                tqdm.write('Failed to generate metadata for: {}, {}'.format(tiffile, e))
                raise
        return coverages

    def build_datasets(self, coverages):
        datasets = []
        for dsdef in tqdm(self.DATASETS):
            # build sorted lists for all attribute filter with None
            # (discriminators)
            # 1. filter coverages by fixed set of filters
            fixed_filter = {
                key: value
                for (key,value) in dsdef['filter'].items()
                if value is not None
            }
            cov_subset = list(filter(
                lambda x: match_coverage(x, fixed_filter),
                coverages
            ))
            # 2. find all possible values for None filters
            #    in filtered coverage subset
            discriminators = {}
            for key in dsdef['filter'].keys():
                if dsdef['filter'][key] is not None:
                    continue
                discriminators[key] = sorted(
                    {cov['bccvl:metadata'][key] for cov in cov_subset if key in cov['bccvl:metadata']}
                )
            # 3. all values collected, lets iterate over the product of them all
            for comb in tqdm(list(product_dict(discriminators))):
                # make a copy
                dsdef2 = copy.copy(dsdef)
                # each comb is one combination of filter values
                dsdef2['filter'].update(comb)
                # generate data subset
                subset = list(filter(
                    lambda x: match_coverage(x, dsdef2['filter']),
                    coverages
                ))
                if not subset:
                    tqdm.write("No Data matched for {}".format(dsdef2['filter']))
                    continue
                coverage = gen_dataset_coverage(subset, dsdef2['aggs'])
                md = self.gen_dataset_metadata(dsdef2, subset)
                md['extent_wgs84'] = get_coverage_extent(coverage)
                coverage['bccvl:metadata'] = md
                coverage['bccvl:metadata']['uuid'] = self.cov_uuid(coverage)
                coverage['bccvl:metadata']['coluuid'] = dsdef2['coluuid']
                datasets.append(coverage)

        return datasets

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--force', action='store_true',
                            help='Re generate data.json form tif files.')
        parser.add_argument('srcdir')
        return parser.parse_args()

    def main(self):
        # TODO: we need a mode to just update as existing json file without parsing
        #       all tiff files. This would be useful to just update titles and
        #       things.
        #       could probably also be done in a separate one off script?
        opts = self.parse_args()
        opts.srcdir = os.path.abspath(opts.srcdir)

        datajson = os.path.join(opts.srcdir, 'data.json')
        tqdm.write("Generate data.json")
        if not os.path.exists(datajson) or opts.force:
            tqdm.write("Build data.json")
            coverages = self.build_data(opts)
            tqdm.write("Write data.json")
            with open(datajson, 'w') as mdfile:
                json.dump(coverages, mdfile, indent=2)
        else:
            tqdm.write("Use existing data.json")
            coverages = json.load(open(datajson))


        tqdm.write("Generate datasets.json")
        datasets = self.build_datasets(coverages)
        tqdm.write("Write datasets.json")
        # save all the data
        with open(os.path.join(opts.srcdir, 'datasets.json'), 'w') as mdfile:
            json.dump(datasets, mdfile, indent=2)

