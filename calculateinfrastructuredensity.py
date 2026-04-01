import geopandas as gpd
import rasterio
from rasterio import features
from rasterio.transform import from_origin
import numpy as np

# 1. Setup paths
shp_path = r"C:\Users\dredhu01\Box\CEE0189\infrastructure_USETHISONE\infrastructure\road\Argentina\argentina_roads.shp"
output_tif = r"C:\Users\dredhu01\Box\CEE0189\infrastructure_USETHISONE\infrastructure\road\Argentina\Argentina_densitypersqkm_500mpixel.tif"

# 2. Load and Project
print("Loading and projecting data...")
gdf = gpd.read_file(shp_path)
if gdf.crs.is_geographic:
    gdf = gdf.to_crs(gdf.estimate_utm_crs())

# 3. Define Raster Grid (500m pixels)
xmin, ymin, xmax, ymax = gdf.total_bounds
pixel_size = 500
width = int((xmax - xmin) / pixel_size)
height = int((ymax - ymin) / pixel_size)
transform = from_origin(xmin, ymax, pixel_size, pixel_size)

# 4. Rasterize: Summing lengths per cell
# We create a list of (geometry, value) tuples where value is the Length in KM
print("Rasterizing road lengths...")
shapes = ((geom, length/1000.0) for geom, length in zip(gdf.geometry, gdf.geometry.length))

# Use MergeAlg.add to SUM lengths if multiple lines touch one pixel
raster = features.rasterize(
    shapes=shapes,
    out_shape=(height, width),
    transform=transform,
    fill=0,
    all_touched=True,
    merge_alg=rasterio.enums.MergeAlg.add,
    dtype='float32'
)

# 5. Calculate Density (km / sq km)
# Each pixel is 0.5km * 0.5km = 0.25 sq km. Multiply sum by 4 to get km/sqkm.
density_raster = raster * 4

# 6. Save to File
print(f"Saving to {output_tif}...")
with rasterio.open(
    output_tif, 'w',
    driver='GTiff',
    height=height,
    width=width,
    count=1,
    dtype='float32',
    crs=gdf.crs,
    transform=transform,
) as dst:
    dst.write(density_raster, 1)

print("Done! Road density calculated successfully.")
