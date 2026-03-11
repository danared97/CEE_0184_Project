import os
import shutil
import numpy as np
import pandas as pd
import geopandas as gpd
import rioxarray
from blackmarble import BlackMarble, Product

# 1. Configuration
#
# Update the path and date range for the Feb-Mar 2026 snowstorm
study_configs = [
    {
        "name": "Snowstorm_Feb_Mar_2026",
        "path": r"C:\Users\dredhu01\Box\CEE0189\studyareas\example_snowstorm.shp",
        "out_dir": r"C:\Users\dredhu01\Box\CEE0189\test_output\bostonsnowstormtake2",
        "start": "2026-02-18",
        "end": "2026-03-02"
    },
]

# Set up the BlackMarble client
# REMINDER: Insert your NASA Earthdata bearer token below
bm = BlackMarble(
    token="eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImRudmFuaHVpcyIsImV4cCI6MTc3NTkzMjA2MSwiaWF0IjoxNzcwNzQ4MDYxLCJpc3MiOiJodHRwczovL3Vycy5lYXJ0aGRhdGEubmFzYS5nb3YiLCJpZGVudGl0eV9wcm92aWRlciI6ImVkbF9vcHMiLCJhY3IiOiJlZGwiLCJhc3N1cmFuY2VfbGV2ZWwiOjN9.MlsRkLkAEiTovHkM8z2O01LGEnGJozR5cu644CSNh9xZ2o5kUPXNRrdD8-g-X2udn7A9NT48C2ZKc_QICrq0ESfmot7xUSbly-f0VdjBc1go-CNmQgdKOr0pAYvJdrh8FexaMdv2mG0GyBdfQNHIxH5DoHdbpwNjA13CRF0mu_WRlll9_QYLq9iHgRyrqtmX-AG9lwJIfloV7tU-WMf6T_oVGgQKlEwKnxiXoUAl2hEEu1jLrR0cY1OLEuO4M8w6aMdhXndPa4aoSPuSi_KUSc228Wfw8Sb3a75e4RcHpZzZIkL1LWFO0s3G_RDIlCPCNqdpLH7egATIF8pix4GKYA",
    output_directory=r"C:\Users\dredhu01\Box\CEE0189\test_output\bostonsnowstormtake2",
    output_skip_if_exists=True
)


# 2. Processing Function
def download_and_process(config):
    print(f"\n>>> Processing: {config['name']}")
    if not os.path.exists(config['out_dir']):
        os.makedirs(config['out_dir'])

    dates = pd.date_range(start=config['start'], end=config['end']).strftime('%Y-%m-%d').tolist()

    # Load shapefile and ensure it matches NASA's WGS84 coordinate system
    gdf = gpd.read_file(config['path']).to_crs("EPSG:4326")

    # Use VNP46A1 (At-sensor Radiance)
    # The library handles spatial resolution matching automatically via the gdf input
    with bm.raster(gdf, product_id=Product.VNP46A1, date_range=dates) as ds:
        # VNP46A1 uses 'DNB_At_Sensor_Radiance' as the primary data layer
        data_in_memory = ds['DNB_At_Sensor_Radiance'].load()

    bin_edges = np.linspace(0, 100, 11)
    daily_records = []

    # Iterate through each day to get daily stats + daily histogram
    for i, date_val in enumerate(data_in_memory.time.values):
        date_str = pd.to_datetime(date_val).strftime('%Y-%m-%d')
        daily_slice = data_in_memory.isel(time=i)

        # Calculate daily histogram
        counts, _ = np.histogram(daily_slice.values.flatten(), bins=bin_edges)
        hist_data = {f"Bin_{int(bin_edges[j])}-{int(bin_edges[j + 1])}": counts[j] for j in range(len(counts))}

        # Create the row for CSV
        row = {
            'Area': config['name'],
            'Date': date_str,
            'Daily_Avg_Radiance': float(daily_slice.mean())
        }
        row.update(hist_data)
        daily_records.append(row)

        # Save individual TIF (aligned to VIIRS 15-arcsecond grid)
        tif_name = f"{config['name']}_{date_str.replace('-', '')}.tif"
        daily_slice.rio.to_raster(os.path.join(config['out_dir'], tif_name))

    print(f"   Finished {len(daily_records)} days for {config['name']}")
    return daily_records


# 3. Execution Loop and Export
all_results = []
for config in study_configs:
    try:
        daily_stats = download_and_process(config)
        all_results.extend(daily_stats)
    except Exception as e:
        print(f"!!! Error on {config['name']}: {e}")

if all_results:
    master_df = pd.DataFrame(all_results)
    final_box_csv = r"C:\Users\dredhu01\Box\CEE0189\test_output\daily_analysis_summary.csv"

    master_df.to_csv(final_box_csv, index=False)
    print(f"\nSUCCESS: Data saved to {final_box_csv}")
