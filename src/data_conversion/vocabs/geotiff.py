from osgeo import gdal


PREDICTORS = {
    gdal.GDT_Unknown: 1,
    gdal.GDT_Byte: 2,
    gdal.GDT_UInt16: 2,
    gdal.GDT_Int16: 2,
    gdal.GDT_UInt32: 2,
    gdal.GDT_Int32: 2,
    gdal.GDT_Float32: 3,
    gdal.GDT_Float64: 3,
    gdal.GDT_CInt16: 2,
    gdal.GDT_CInt32: 2,
    gdal.GDT_CFloat32: 3,
    gdal.GDT_CFloat64: 3
}
