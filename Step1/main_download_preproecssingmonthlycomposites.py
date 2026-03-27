import os
import time
import blackmarble
import numpy as np
import shutil
import pandas as pd
import geopandas as gpd
#import Rbeast
import rioxarray

from blackmarble import BlackMarble, Product


# define region of interest
# define as three composites: pre-fire, during fire, and post-fire
# monthly composites: 24-month window around fire dates
study_configs = [
    {"name": "California", "path": "C:/Users/dredhu01/Box/CEE0189/studyareas/cali_2.shp", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/monthly/California", "fire_date": "2025-01-07"},
    {"name": "argentina", "path": "C:/Users/dredhu01/Box/CEE0189/studyareas/argentina_3.shp", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/monthly/Argentina", "fire_date": "2025-01-15"},
    {"name": "southkorea", "path": "C:/Users/dredhu01/Box/CEE0189/studyareas/southkorea.shp", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/monthly/SouthKorea", "fire_date": "2025-03-21"},
]

#set up blackmarble client - need to have an Earthdata access token that allows clearance to LAADS DAAC
bm = BlackMarble(
    token="eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImRudmFuaHVpcyIsImV4cCI6MTc3NTkzMjA2MSwiaWF0IjoxNzcwNzQ4MDYxLCJpc3MiOiJodHRwczovL3Vycy5lYXJ0aGRhdGEubmFzYS5nb3YiLCJpZGVudGl0eV9wcm92aWRlciI6ImVkbF9vcHMiLCJhY3IiOiJlZGwiLCJhc3N1cmFuY2VfbGV2ZWwiOjN9.MlsRkLkAEiTovHkM8z2O01LGEnGJozR5cu644CSNh9xZ2o5kUPXNRrdD8-g-X2udn7A9NT48C2ZKc_QICrq0ESfmot7xUSbly-f0VdjBc1go-CNmQgdKOr0pAYvJdrh8FexaMdv2mG0GyBdfQNHIxH5DoHdbpwNjA13CRF0mu_WRlll9_QYLq9iHgRyrqtmX-AG9lwJIfloV7tU-WMf6T_oVGgQKlEwKnxiXoUAl2hEEu1jLrR0cY1OLEuO4M8w6aMdhXndPa4aoSPuSi_KUSc228Wfw8Sb3a75e4RcHpZzZIkL1LWFO0s3G_RDIlCPCNqdpLH7egATIF8pix4GKYA",
    output_directory="C:/Users/dredhu01/Box/CEE0189/output/step3_backup/monthly",
    output_skip_if_exists=True
)

# Function: runs through each study area and returns a monthly mosaicked TIF output for full area
# Modified from daily → monthly composites (12 months before fire, 12 months after)

base_box_path = "C:/Users/dredhu01/Box/CEE0189/output/step3_backup/monthly"

def process_monthly_slice(config, raster, month_str, bin_edges):
    values = raster.values.flatten()
    values = values[~np.isnan(values)]

    hist, _ = np.histogram(values, bins=bin_edges)

    return {
        "study_area": config["name"],
        "month": month_str,
        "mean_radiance": float(np.mean(values)),
        "median_radiance": float(np.median(values)),
        "histogram": hist.tolist()
    }


def download_and_process(config):
    print(f"\n>>> Processing: {config['name']}")
    if not os.path.exists(config['out_dir']): os.makedirs(config['out_dir'])

    fire_date = pd.to_datetime(config["fire_date"])

    # create 24-month window (12 before, 12 after)
    start = (fire_date - pd.DateOffset(months=12)).replace(day=1)
    end = (fire_date + pd.DateOffset(months=12)).replace(day=1)

    months = pd.period_range(start=start, end=end, freq='M')

    gdf = gpd.read_file(config['path'])
    monthly_records = []
    bin_edges = np.linspace(0, 100, 11)

    for month in months:
        start_date = month.start_time.strftime('%Y-%m-%d')
        end_date = month.end_time.strftime('%Y-%m-%d')

        print(f"   Processing month: {month}")

        try:
            with bm.raster(gdf, product_id=Product.VNP46A2, date_range=[start_date, end_date]) as ds:
                data_in_memory = ds['DNB_BRDF-Corrected_NTL'].load()

            # create monthly composite (median is more robust than mean for VIIRS)
            monthly_composite = data_in_memory.median(dim='time')

            # save raster (optional but useful for later analysis)
            out_tif = os.path.join(config['out_dir'], f"{config['name']}_{month}.tif")
            monthly_composite.rio.to_raster(out_tif)

            monthly_records.append(
                process_monthly_slice(config, monthly_composite, str(month), bin_edges)
            )

        except Exception as e:
            print(f"   SKIPPING {month}: ({e})")

    return monthly_records


# execute function
all_results = []
for config in study_configs:
    try:
        monthly_stats = download_and_process(config)
        all_results.extend(monthly_stats)
    except Exception as e:
        print(f"!!! Error on {config['name']}: {e}")

# 4. export csv (Will have one row per month per area)
if all_results:
    master_df = pd.DataFrame(all_results)
    local_csv = "C:/Users/dredhu01/Desktop/monthly_viirs.csv"
    #was having issues saving to box, this just saves locally and then to box
    final_box_csv = "C:/Users/dredhu01/Box/CEE0189/output/monthly_viirs.csv"

    master_df.to_csv(local_csv, index=False)
    shutil.copy2(local_csv, final_box_csv)
    print(f"\nSUCCESS: CSV with monthly radiance stats saved to {final_box_csv}")