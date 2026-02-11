import os
import time
import blackmarble
import pandas as pd
import geopandas as gpd
import rioxarray

from blackmarble import BlackMarble, Product

# ------------------------------------------------------------------------------
# 1. Define your region of interest
# ------------------------------------------------------------------------------
# input shapefile here
study_configs = [
    {"name": "Cali", "path": "C:/Users/dredhu01/Box/CEE0189/studyareas/cali_test.shp", "out_dir": "C:/Users/dredhu01/Box/CEE0189/test_output", "start": "2025-01-01", "end": "2025-02-11"},
]

# ------------------------------------------------------------------------------
# 2. Set up the BlackMarble client
# ------------------------------------------------------------------------------
# If the environment variable `BLACKMARBLE_TOKEN` is set, it will be used automatically.
# You can also pass your token directly, but using the environment variable is recommended.
bm = BlackMarble(token="eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImRudmFuaHVpcyIsImV4cCI6MTc3NTkzMjA2MSwiaWF0IjoxNzcwNzQ4MDYxLCJpc3MiOiJodHRwczovL3Vycy5lYXJ0aGRhdGEubmFzYS5nb3YiLCJpZGVudGl0eV9wcm92aWRlciI6ImVkbF9vcHMiLCJhY3IiOiJlZGwiLCJhc3N1cmFuY2VfbGV2ZWwiOjN9.MlsRkLkAEiTovHkM8z2O01LGEnGJozR5cu644CSNh9xZ2o5kUPXNRrdD8-g-X2udn7A9NT48C2ZKc_QICrq0ESfmot7xUSbly-f0VdjBc1go-CNmQgdKOr0pAYvJdrh8FexaMdv2mG0GyBdfQNHIxH5DoHdbpwNjA13CRF0mu_WRlll9_QYLq9iHgRyrqtmX-AG9lwJIfloV7tU-WMf6T_oVGgQKlEwKnxiXoUAl2hEEu1jLrR0cY1OLEuO4M8w6aMdhXndPa4aoSPuSi_KUSc228Wfw8Sb3a75e4RcHpZzZIkL1LWFO0s3G_RDIlCPCNqdpLH7egATIF8pix4GKYA")

# ------------------------------------------------------------------------------
# 3. Download VNP46 data from NASA Earthdata
# ------------------------------------------------------------------------------
# updated function so it runs through each study area and saves as a tif
def download_and_save_daily(config):
    print(f"Processing {config['name']}...")

    # Create the date list
    dates = pd.date_range(start=config['start'], end=config['end']).strftime('%Y-%m-%d').tolist()
    gdf = gpd.read_file(config['path'])

    # Download the multi-date dataset
    ds = bm.raster(gdf, product_id=Product.VNP46A2, date_range=dates)

    # 2. Iterate through each time slice and save as a TIF
    # Assuming 'AllAngle_Composite_Snow_Free' is the variable name
    var_name = 'AllAngle_Composite_Snow_Free'

    with bm.raster(gdf, product_id=Product.VNP46A2, date_range=dates) as ds:
        var_name = 'AllAngle_Composite_Snow_Free'
        for i, date_val in enumerate(ds.time.values):
            date_str = pd.to_datetime(date_val).strftime('%Y%m%d')
            file_path = os.path.join(config['out_dir'], f"{config['name']}_{date_str}.tif")

            daily_band = ds[var_name].isel(time=i)
            daily_band.rio.to_raster(file_path)
            ds.close()
            time.sleep(2)


        # 3. Run for all areas
for config in study_configs:
    download_and_save_daily(config)