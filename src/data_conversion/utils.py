

# convert pixel to projection unit
def transform_pixel(ds, x, y):
    xoff, a, b, yoff, d, e = ds.GetGeoTransform()
    return (
        # round lat/lon to 5 digits which is about 1cm at equator
        round(a * x + b * y + xoff, 5),
        round(d * x + e * y + yoff, 5),
    )
