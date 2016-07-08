#!/usr/bin/env python
import os
import os.path
import glob
import json
import tempfile
import shutil
import sys
import re
import zipfile


JSON_TEMPLATE = "geofabric.template.json"
JSON_TEMPLATE_ATTR = "geofabric_attr.template.json"

cats = [('SH_Network.gdb.zip', 'catchment', 'ahgfcatchment'), 
        ('SH_Network.gdb.zip' , 'stream', 'ahgfnetworkstream')]

layers = {
    'catchment': [('stream_attributesv1.1.5.gdb.zip', 'climate', 'climate_lut', u'Climate Data from 9" DEM of Australia version 3 (2008), ANUCLIM (Fenner School)'), 
                  ('stream_attributesv1.1.5.gdb.zip', 'vegetation', 'veg_lut', u'NVIS Major Vegetation sub-groups version 3.1'),
                  ('stream_attributesv1.1.5.gdb.zip', 'substrate', 'substrate_lut', u'Surface geology of Australia 1:1M'), 
                  ('stream_attributesv1.1.5.gdb.zip', 'terrain', 'terrain_lut', u'Terrain Data from 9" DEM of Australia version 3 (2008)'),
                  ('stream_attributesv1.1.5.gdb.zip', 'landuse', 'landuse_lut', u'Catchment Scale Land Use Mapping for Australia Update (CLUM Update 04/09)'),
                  ('stream_attributesv1.1.5.gdb.zip', 'population', 'landuse_lut', u'ABS Population density within 2006 Australian Standard Geographic Classification census collector districts'),
                  ('stream_attributesv1.1.5.gdb.zip', 'npp', 'npp_lut', u'Net Primary Production (pre-1788)'),
                  ('stream_attributesv1.1.5.gdb.zip', 'rdi', 'rdi_geodata2_lut', u'River Disturbance Indeces and Factors')],
    'stream':    [('stream_attributesv1.1.5.gdb.zip', 'climate', 'climate_lut', u'Climate Data from 9" DEM of Australia version 3 (2008), ANUCLIM (Fenner School)'), 
                  ('stream_attributesv1.1.5.gdb.zip', 'vegetation', 'veg_lut', u'NVIS Major Vegetation sub-groups version 3.1'),
                  ('stream_attributesv1.1.5.gdb.zip', 'substrate', 'substrate_lut', u'Surface geology of Australia 1:1M'), 
                  ('stream_attributesv1.1.5.gdb.zip', 'terrain', 'terrain_lut', u'Terrain Data from 9" DEM of Australia version 3 (2008)'),
                  ('stream_attributesv1.1.5.gdb.zip', 'landuse', 'landuse_lut', u'Catchment Scale Land Use Mapping for Australia Update (CLUM Update 04/09)'),
                  ('stream_attributesv1.1.5.gdb.zip', 'population', 'landuse_lut', u'ABS Population density within 2006 Australian Standard Geographic Classification census collector districts'),
                  ('stream_attributesv1.1.5.gdb.zip', 'network', 'network_lut', u'Stream Network from AusHydro version 1.1.6'),
                  ('stream_attributesv1.1.5.gdb.zip', 'connectivity', 'connectivity_lut', u'Stream Connectivity from AusHydro version 1.1.6')]
}

# Attributes for dataset
attributes = {
    'catchment': {
        'climate': ['catannrad', 'catanntemp', 'catcoldmthmin', 'cathotmthmax', 'catannrain', 'catdryqrain', 
                    'catwetqrain', 'catwarmqrain', 'catcoldqrain', 'catcoldqtemp', 'catdryqtemp', 'catwetqtemp',
                    'catanngromega', 'catanngromeso', 'catanngromicro', 'catgromegaseas', 'catgromesoseas', 
                    'catgromicroseas', 'caterosivity'],
        'vegetation': ['catbare_ext', 'catforests_ext', 'catgrasses_ext', 'catnodata_ext', 'catwoodlands_ext', 
                       'catshrubs_ext', 'catbare_nat', 'catforests_nat', 'catgrasses_nat', 'catnodata_nat', 
                       'catwoodlands_nat', 'catshrubs_nat'],
        'substrate': ['cat_carbnatesed', 'cat_igneous', 'cat_metamorph', 'cat_oldrock', 'cat_othersed', 
                      'cat_sedvolc', 'cat_silicsed', 'cat_unconsoldted', 'cat_a_ksat', 'cat_solpawhc'],
        'terrain': ['catarea', 'catelemax', 'catelemean', 'catrelief', 'catslope', 'catstorage',
                    'elongratio', 'reliefratio'],
        'npp': ['nppbaseann', 'nppbase1', 'nppbase2', 'nppbase3', 'nppbase4', 'nppbase5', 'nppbase6', 
                'nppbase7', 'nppbase8', 'nppbase9', 'nppbase10', 'nppbase11', 'nppbase12'],
        'landuse': ['cat_aqu', 'cat_artimp', 'cat_drainage', 'cat_fert', 'cat_forstry', 'cat_intan', 'cat_intpl', 
                    'cat_irr', 'cat_mining', 'cat_mod', 'cat_pest', 'cat_road', 'cat_urban'],
        'population': ['catpop_gt_1', 'catpop_gt_10', 'catpopmax', 'catpopmean'],
        'rdi': ['sfrdi','imf', 'fdf', 'scdi', 'ef', 'if', 'sf', 'nwisf', 'gdsf', 'sdsf',
                'nwiif', 'gdif', 'sdif', 'nwief', 'gdef', 'sdef', 'luf', 'lbf', 'nwiimf',
                'gdimf', 'sdimf', 'nwifdf', 'gdfdf', 'sdfdf', 'cdi', 'frdi', 'rdi']
    }, 
    'stream': {
        'climate': ['strannrad', 'stranntemp', 'strcoldmthmin', 'strhotmthmax', 'strannrain', 'strdryqrain', 
                    'strwetqrain', 'strwarmqrain', 'strcoldqrain', 'strcoldqtemp', 'strdryqtemp', 'strwetqtemp',
                    'stranngromega', 'stranngromeso', 'stranngromicro', 'strgromegaseas', 'strgromesoseas', 
                    'strgromicroseas', 'suberosivity'],
        'vegetation': ['strbare_ext', 'strforests_ext', 'strgrasses_ext', 'strnodata_ext', 'strwoodlands_ext', 
                       'strshrubs_ext', 'strbare_nat', 'strforests_nat', 'strgrasses_nat', 'strnodata_nat', 
                       'strwoodlands_nat', 'strshrubs_nat'],
        'substrate': ['str_carbnatesed', 'str_igneous', 'str_metamorph', 'str_oldrock', 'str_othersed', 
                      'str_sedvolc', 'str_silicsed', 'str_unconsoldted', 'str_a_ksat', 'str_sanda',
                      'str_claya', 'str_clayb'],
        'terrain': ['strahler', 'strelemax', 'strelemean', 'strelemin', 'valleyslope', 'downavgslp',
                    'downmaxslp', 'upsdist', 'd2outlet', 'aspect', 'confinement'],
        'landuse': ['str_aqu', 'str_artimp', 'str_drainage', 'str_fert', 'str_forstry', 'str_intan', 'str_intpl',
                    'str_irr', 'str_mining', 'str_mod', 'str_pest', 'str_road', 'str_urban'], 
        'population': ['strpop_gt_1', 'strpop_gt_10', 'strpopmax', 'strpopmean'],
        'network': ['strdensity', 'no_waterholes', 'km_waterholes', 'no_springs', 'km_springs', 'a_lakes',
                    'km_lakes', 'a_wcourse', 'km_wcourse', 'lakes', 'springs', 'watcrsarea', 'waterholes',
                    'wateryness', 'rchlen'],
        'connectivity': ['conlen', 'dupreservr', 'd2reservor', 'barrierdown', 'barrierup', 'distupdamw', 'd2damwall', 
                         'conlenres', 'conlendam', 'artfbarier', 'totlen', 'cliffdown', 'cliffup', 'waterfall',
                         'wfalldown', 'waterfallup']        
    }
}



def create_target_dir(basename):
    """create zip folder structure in tmp location.
    return root folder
    """
    tmpdir = tempfile.mkdtemp(prefix=basename)
    os.mkdir(os.path.join(tmpdir, basename))
    os.mkdir(os.path.join(tmpdir, basename, 'data'))
    os.mkdir(os.path.join(tmpdir, basename, 'bccvl'))
    return tmpdir

def gen_metadatajson(template, ziproot, basename, baselayer, attrlayer, dbfilename):
    """read metadata template and populate rest of fields
    and write to ziproot + '/bccvl/metadata.json'
    """
    md = json.load(open(template, 'r'))

    base_filename, baselyrname, basetable = baselayer
    attr_filename, layername, attrtable, attrdesc = attrlayer
        
    # update dataset info
    resolution = '9 arcsec'
    if layername == 'climate':
        md['title'] = 'Geofabric climate dataset ({cat}) {resolution} (2008)'.format(cat=baselyrname, resolution=resolution)
        md['resolution'] = resolution

    else:
        md['title'] = md['title'].format(layername=layername, cat=baselyrname)
    md['descriptions'] = attrdesc

    # TODO: Shapefile has a max column length of 10 characters.
    # The layername is the new table name which is same as basename
    md['layers'] = ["{bfname}-{cat}.{afname}-{layername}.{attr}".format( 
            bfname=base_filename, cat=basetable, afname=dbfilename, layername=basename, attr=attr)
            for attr in truncate_name(attributes[baselyrname][layername])]
    md['foreignKey'] = "segmentno"
    md['base_filename'] = base_filename
    md['attribute_filename'] = dbfilename

    mdfile = open(os.path.join(ziproot, basename, 'bccvl', 'metadata.json'), 'w')
    json.dump(md, mdfile, indent=4)
    mdfile.close()

def ogr_extract(attrfile, attrtable, attrlist, dest):
    """Use ogr2ogr to extract relevant attributes from src file to dest"""
    # The new table will have the same name as the output file
    sqlcmd = 'select segmentno, {attributes} from {attrtable}'.format(attributes=','.join(attrlist), attrtable=attrtable)
    ret = os.system('ogr2ogr -f "ESRI Shapefile" {outfile} {attrfile} {attrtable} -sql "{select}"'.format(
                    outfile=dest, attrfile=attrfile, attrtable=attrtable, select=sqlcmd))
    if ret != 0:
        raise Exception("can't extract attributes '{0}' from '{1}' ({2})".format(','.join(attrlist), attrtable, ret))

def convert(srcdir, ziproot, basename, baselayer, attrlayer, destfilename):
    """Extract relevant attributes from the attribute table, and save it as Shapefile
    """
    # attribute file
    attrfile = os.path.join(srcdir, attrlayer[0])
    attrlist = attributes.get(baselayer[1], {}).get(attrlayer[1], [])
    ogr_extract(attrfile, attrlayer[2], attrlist, os.path.join(ziproot, basename, 'data', destfilename))


def zipbccvldataset(ziproot, destdir, basename):
    zipname = os.path.abspath(os.path.join(destdir, basename + '.zip'))
    cwd = os.getcwd()
    os.chdir(ziproot)
    zipdir(basename, zipname)
    os.chdir(cwd)

def zipdir(path, zipfilename):
    try:
        zipf = zipfile.ZipFile(zipfilename, 'w', zipfile.ZIP_DEFLATED)
        # zipf is zipfile handle
        for root, dirs, files in os.walk(path):
            for file in files:
                zipf.write(os.path.join(root, file))
    except Exception as e:
        raise Exception("can't zip {0}: {1}".format(ziproot, str(e)))
    finally:
        zipf.close()

def truncate_name(namelist, maxchar=10):
    # Truncate the field names as during the conversion from gdb to shapefile would using ogr2ogr.
    # TODO: Check this work for all cases
    truncatedList = [name[:maxchar] for name in namelist]
    for i in range(len(truncatedList)):
        index = [i]
        for j in range(len(truncatedList)):
            if i != j and truncatedList[j] == truncatedList[i]:
                index.append(j)
        # Change name again; the last 2 character is _{digit}
        if len(index) > 1:
            for k in range(1, len(index)):
                tname = truncatedList[index[k]][:(maxchar-2)] + "_{}".format(k)
                truncatedList[index[k]] = tname
    return truncatedList

def main(argv):
    ziproot = None
    srcdir = None
    try:
        if len(argv) != 3:
            print "Usage: {0} <srczip> <destdir>".format(argv[0])
            sys.exit(1)
        srcdir = argv[1]
        destdir = argv[2]

        # Geospatial item as zip file
        basefilename = 'SH_Network.gdb.zip'
        base_dir = 'geofabric_geospatial.gdb'   # gdb file
        ziproot = create_target_dir(base_dir)

        # Copy file to the data directory
        shutil.copy(os.path.join(srcdir, basefilename), os.path.join(ziproot, base_dir, 'data'))
        shutil.copy(JSON_TEMPLATE, os.path.join(ziproot, base_dir, 'bccvl', 'metadata.json'))

        zipbccvldataset(ziproot, destdir, base_dir)
        if ziproot:
            shutil.rmtree(ziproot)

        # Generate Geofabric attribute dataset as zip file
        for baselayer in cats:
            for attrlayer in layers[baselayer[1]]:
                base_dir = '{baselayer}_{attrlayer}'.format(baselayer=baselayer[1], attrlayer=attrlayer[1])
                shpfilename = base_dir + '.dbf'  # shape file
                ziproot = create_target_dir(base_dir)

                # Extract the layer from attribute file and save as Shapefile
                convert(srcdir, ziproot, base_dir, baselayer, attrlayer, shpfilename)

                # Generate metadata
                gen_metadatajson(JSON_TEMPLATE_ATTR, ziproot, base_dir, baselayer, attrlayer, shpfilename)
                zipbccvldataset(ziproot, destdir, base_dir)
                if ziproot:
                    shutil.rmtree(ziproot)
    except Exception as e:
        print "Fail to convert: ", e
    finally:
        # cleanup temp location
        if ziproot and os.path.exists(ziproot):
            shutil.rmtree(ziproot)


if __name__ == '__main__':
    main(sys.argv)