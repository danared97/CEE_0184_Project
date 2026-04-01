import os
import time
import blackmarble
import numpy as np
import shutil
import pandas as pd
import geopandas as gpd
import rioxarray
from shapely.geometry import mapping

from blackmarble import BlackMarble, Product


# define region of interest
# define as three date ranges: pre-fire, during fire, and post-fire
# goal is to download daily composite data of each study area from Earthdata through an API token
study_configs = [
    {"name": "argentina_prefire", "path": "H:/argentina_take2/argentinastudyarea/argentina/argentina_studyarea.shp", "out_dir": "H:/argentina_take2/argentinastudyarea/argentina/daily/prefire", "start": "2025-01-08", "end": "2025-01-14"},
    {"name": "argentina_fire", "path": "H:/argentina_take2/argentinastudyarea/argentina/argentina_studyarea.shp", "out_dir": "H:/argentina_take2/argentinastudyarea/argentina/daily/fire", "start": "2025-01-15", "end": "2025-02-23"},
    {"name": "argentina_postfire", "path": "H:/argentina_take2/argentinastudyarea/argentina/argentina_studyarea.shp", "out_dir": "H:/argentina_take2/argentinastudyarea/argentina/daily/postfire", "start": "2025-02-24", "end": "2025-03-03"},
]

#set up blackmarble client - need to have an Earthdata access token that allows clearance to LAADS DAAC
bm = BlackMarble(
    token="eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImRudmFuaHVpcyIsImV4cCI6MTc3NTkzMjA2MSwiaWF0IjoxNzcwNzQ4MDYxLCJpc3MiOiJodHRwczovL3Vycy5lYXJ0aGRhdGEubmFzYS5nb3YiLCJpZGVudGl0eV9wcm92aWRlciI6ImVkbF9vcHMiLCJhY3IiOiJlZGwiLCJhc3N1cmFuY2VfbGV2ZWwiOjN9.MlsRkLkAEiTovHkM8z2O01LGEnGJozR5cu644CSNh9xZ2o5kUPXNRrdD8-g-X2udn7A9NT48C2ZKc_QICrq0ESfmot7xUSbly-f0VdjBc1go-CNmQgdKOr0pAYvJdrh8FexaMdv2mG0GyBdfQNHIxH5DoHdbpwNjA13CRF0mu_WRlll9_QYLq9iHgRyrqtmX-AG9lwJIfloV7tU-WMf6T_oVGgQKlEwKnxiXoUAl2hEEu1jLrR0cY1OLEuO4M8w6aMdhXndPa4aoSPuSi_KUSc228Wfw8Sb3a75e4RcHpZzZIkL1LWFO0s3G_RDIlCPCNqdpLH7egATIF8pix4GKYA",
    output_directory="C:/Users/dredhu01/Box/CEE0189/output/step3_backup/daily",
    output_skip_if_exists=True
)

def process_daily_slice(config, raster, date_str, bin_edges):
    values = raster.values.flatten()
    values = values[~np.isnan(values)]
    if len(values) == 0:
        return {"study_area": config["name"], "date": date_str, "mean_radiance": 0, "median_radiance": 0,
                "histogram": []}

    hist, _ = np.histogram(values, bins=bin_edges)
    return {
        "study_area": config["name"],
        "date": date_str,
        "mean_radiance": float(np.mean(values)),
        "median_radiance": float(np.median(values)),
        "histogram": hist.tolist()
    }

def download_and_process(config):
    print(f"\n>>> Processing: {config['name']}")
    if not os.path.exists(config['out_dir']): os.makedirs(config['out_dir'])

    dates = pd.date_range(start=config['start'], end=config['end']).strftime('%Y-%m-%d').tolist()

    # load shapefile and ensure CRS is WGS84
    gdf = gpd.read_file(config['path']).to_crs("EPSG:4326")

    daily_records = []
    bin_edges = np.linspace(0, 100, 11)

    try:
        print(f"   Attempting bulk download...")
        with bm.raster(gdf, product_id=Product.VNP46A2, date_range=dates) as ds:
            data_in_memory = ds['DNB_BRDF-Corrected_NTL'].load()

        for i, date_val in enumerate(data_in_memory.time.values):
            date_str = pd.to_datetime(date_val).strftime('%Y-%m-%d')
            daily_slice = data_in_memory.isel(time=i)

            # Assign CRS then clip to the actual polygon
            daily_slice.rio.write_crs("EPSG:4326", inplace=True)
            if 'lat' in daily_slice.coords:
                daily_slice = daily_slice.rename({'lat': 'y', 'lon': 'x'})

            # explicitly clip to the study area
            clipped_slice = daily_slice.rio.clip(gdf.geometry.apply(mapping), gdf.crs)

            # save out
            out_tif = os.path.join(config['out_dir'], f"{config['name']}_{date_str}.tif")
            clipped_slice.rio.to_raster(out_tif)

            # run stats save to csv
            daily_records.append(process_daily_slice(config, clipped_slice, date_str, bin_edges))

    except Exception as e:
        print(f"   Error: {e}. Switching to individual day collection...")
        for date_str in dates:
            try:
                with bm.raster(gdf, product_id=Product.VNP46A2, date_range=date_str) as ds:
                    daily_slice = ds['DNB_BRDF-Corrected_NTL'].load().isel(time=0)

                daily_slice.rio.write_crs("EPSG:4326", inplace=True)
                if 'lat' in daily_slice.coords:
                    daily_slice = daily_slice.rename({'lat': 'y', 'lon': 'x'})

                # Clip individual daily slice
                clipped_slice = daily_slice.rio.clip(gdf.geometry.apply(mapping), gdf.crs)

                out_tif = os.path.join(config['out_dir'], f"{config['name']}_{date_str}.tif")
                clipped_slice.rio.to_raster(out_tif)
                daily_records.append(process_daily_slice(config, clipped_slice, date_str, bin_edges))
            except Exception as day_error:
                print(f"   SKIPPING {date_str}: {day_error}")

    return daily_records

all_results = []
for config in study_configs:
    try:
        daily_stats = download_and_process(config)
        all_results.extend(daily_stats)
    except Exception as e:
        print(f"!!! Critical error on {config['name']}: {e}")

# final export
if all_results:
    master_df = pd.DataFrame(all_results)
    local_csv = "H:/argentina_take2/argentinastudyarea/argentina/daily_viirs.csv"
    master_df.to_csv(local_csv, index=False)
    print(f"\nSUCCESS: CSV with daily stats saved to {local_csv}")
