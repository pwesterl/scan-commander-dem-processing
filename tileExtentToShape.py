import os
import glob
import argparse
from osgeo import gdal, ogr, osr


def get_raster_extent(raster_path):
    ds = gdal.Open(raster_path)
    if ds is None:
        return None

    gt = ds.GetGeoTransform()
    width = ds.RasterXSize
    height = ds.RasterYSize

    minx = gt[0]
    maxy = gt[3]
    maxx = minx + gt[1] * width
    miny = maxy + gt[5] * height

    projection = ds.GetProjection()
    ds = None

    return {
        "minx": minx,
        "miny": miny,
        "maxx": maxx,
        "maxy": maxy,
        "projection": projection,
        "filename": os.path.basename(raster_path),
        "filepath": os.path.abspath(raster_path), 
    }


def create_extent_shapefile(input_path, output_shapefile, pattern="*.tif"):
    print("I egna fileExtentToShape")
    if not os.path.exists(input_path):
        raise RuntimeError(f"No file found matching {input_path}")
    
    # Ensure parent directory exists
    out_dir = os.path.dirname(output_shapefile)
    os.makedirs(out_dir, exist_ok=True)

    e = get_raster_extent(input_path)
    srs = osr.SpatialReference()
    srs.ImportFromWkt(e["projection"])

    driver = ogr.GetDriverByName("ESRI Shapefile")
    if os.path.exists(output_shapefile):
        driver.DeleteDataSource(output_shapefile)

    ds = driver.CreateDataSource(output_shapefile)
    layer = ds.CreateLayer("extents", srs, ogr.wkbPolygon)


    fields = [
        ("filename", ogr.OFTString),   
        ("filepath", ogr.OFTString),  
        ("minx", ogr.OFTReal),
        ("miny", ogr.OFTReal),
        ("maxx", ogr.OFTReal),
        ("maxy", ogr.OFTReal),
        ("width", ogr.OFTReal),
        ("height", ogr.OFTReal),
    ]
    for name, ftype in fields:
        field = ogr.FieldDefn(name, ftype)
        if name == "filepath":
            field.SetWidth(254)  
        layer.CreateField(field)

    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(e["minx"], e["miny"])
    ring.AddPoint(e["maxx"], e["miny"])
    ring.AddPoint(e["maxx"], e["maxy"])
    ring.AddPoint(e["minx"], e["maxy"])
    ring.AddPoint(e["minx"], e["miny"])

    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)

    feat = ogr.Feature(layer.GetLayerDefn())
    feat.SetGeometry(poly)
    feat.SetField("filename", e["filename"])
    feat.SetField("filepath", e["filepath"])  
    feat.SetField("minx", e["minx"])
    feat.SetField("miny", e["miny"])
    feat.SetField("maxx", e["maxx"])
    feat.SetField("maxy", e["maxy"])
    feat.SetField("width", e["maxx"] - e["minx"])
    feat.SetField("height", e["maxy"] - e["miny"])
    layer.CreateFeature(feat)

    ds = None
    print(f"Created shapefile: {output_shapefile}")


def create_merged_extent_shapefile(input_dir, output_shapefile, pattern="*.tif"):
    #TODO: modify this function to accept input_path as param instead of dir.
    raster_files = glob.glob(os.path.join(input_dir, pattern))
    if not raster_files:
        raise RuntimeError("No raster files found")

    extents = [get_raster_extent(r) for r in raster_files if get_raster_extent(r)]
    projection = extents[0]["projection"]

    minx = min(e["minx"] for e in extents)
    miny = min(e["miny"] for e in extents)
    maxx = max(e["maxx"] for e in extents)
    maxy = max(e["maxy"] for e in extents)

    srs = osr.SpatialReference()
    srs.ImportFromWkt(projection)

    driver = ogr.GetDriverByName("ESRI Shapefile")
    if os.path.exists(output_shapefile):
        driver.DeleteDataSource(output_shapefile)

    ds = driver.CreateDataSource(output_shapefile)
    layer = ds.CreateLayer("merged_extent", srs, ogr.wkbPolygon)

    layer.CreateField(ogr.FieldDefn("num_tiles", ogr.OFTInteger))

    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(minx, miny)
    ring.AddPoint(maxx, miny)
    ring.AddPoint(maxx, maxy)
    ring.AddPoint(minx, maxy)
    ring.AddPoint(minx, miny)

    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)

    feat = ogr.Feature(layer.GetLayerDefn())
    feat.SetGeometry(poly)
    feat.SetField("num_tiles", len(extents))
    layer.CreateFeature(feat)

    ds = None
    print(f"Created merged extent shapefile: {output_shapefile}")


def main():
    parser = argparse.ArgumentParser(
        description="Create extent shapefile(s) from raster tiles"
    )
    parser.add_argument("input_dir", help="Directory containing raster files")
    parser.add_argument("output_shapefile", help="Output shapefile path")
    parser.add_argument(
        "--pattern", default="*.tif", help="Raster filename pattern (default: *.tif)"
    )
    parser.add_argument(
        "--merged",
        action="store_true",
        help="Create a single merged extent instead of per-tile extents",
    )

    args = parser.parse_args()

    if args.merged:
        create_merged_extent_shapefile(
            args.input_dir, args.output_shapefile, args.pattern
        )
    else:
        create_extent_shapefile(
            args.input_dir, args.output_shapefile, args.pattern
        )


if __name__ == "__main__":
    main()
