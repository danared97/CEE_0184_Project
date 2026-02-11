import blackmarble
import geopandas as gpd

from blackmarble import BlackMarble, Product

# ------------------------------------------------------------------------------
# 1. Define your region of interest
# ------------------------------------------------------------------------------
# In this example ,we load a region from a GeoJSON.
gdf = gpd.read_file("path/to/your/shapefile.geojson")

# ------------------------------------------------------------------------------
# 2. Set up the BlackMarble client
# ------------------------------------------------------------------------------
# If the environment variable `BLACKMARBLE_TOKEN` is set, it will be used automatically.
# You can also pass your token directly, but using the environment variable is recommended.
bm = BlackMarble(token="YOUR_BLACKMARBLE_TOKEN")

# ------------------------------------------------------------------------------
# 3. Download VNP46 data from NASA Earthdata
# ------------------------------------------------------------------------------
# In this example, we request the VNP46A2 product for a specific date.
# The data is returned as an xarray.Dataset.
raster_earth_day = bm.raster(
    gdf,
    product_id=Product.VNP46A2,
    date_range="2025-04-22",
)