import os
import time
import blackmarble
import numpy as np
import shutil
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
bm = BlackMarble(
    token="....",
    output_directory="C:/Users/dredhu01/Box/CEE0189/test_output", # Choose any local folder on your C: drive
    output_skip_if_exists=True
)
# ------------------------------------------------------------------------------
# 3. Download VNP46 data from NASA Earthdata
# ------------------------------------------------------------------------------
# updated function so it runs through each study area and saves as a tif
# Use this name consistently
# 2. CONFIGURE YOUR 6 AREAS HERE
# Ensure "out_dir" is a unique subfolder for each area inside your Box path
base_box_path = "C:/Users/dredhu01/Box/CEE0189/test_output"

# 3. THE PROCESSING FUNCTION
def download_and_process(config):
    print(f"\n>>> Processing: {config['name']}")
    if not os.path.exists(config['out_dir']): os.makedirs(config['out_dir'])

    dates = pd.date_range(start=config['start'], end=config['end']).strftime('%Y-%m-%d').tolist()
    gdf = gpd.read_file(config['path'])

    with bm.raster(gdf, product_id=Product.VNP46A2, date_range=dates) as ds:
        data_in_memory = ds['DNB_BRDF-Corrected_NTL'].load()

    # Define bins: 0-10, 10-20 ... 90-100
    bin_edges = np.linspace(0, 100, 11)
    daily_records = []

    # Iterate through each day to get daily stats + daily histogram
    for i, date_val in enumerate(data_in_memory.time.values):
        date_str = pd.to_datetime(date_val).strftime('%Y-%m-%d')
        daily_slice = data_in_memory.isel(time=i)

        # Calculate histogram for THIS day only
        counts, _ = np.histogram(daily_slice.values.flatten(), bins=bin_edges)
        hist_data = {f"Bin_{int(bin_edges[j])}-{int(bin_edges[j + 1])}": counts[j] for j in range(len(counts))}

        # Create the row for this specific day
        row = {
            'Area': config['name'],
            'Date': date_str,
            'Daily_Avg_Radiance': float(daily_slice.mean())
        }
        row.update(hist_data)  # Add the daily histogram bins
        daily_records.append(row)

        # Save individual TIF
        tif_name = f"{config['name']}_{date_str.replace('-', '')}.tif"
        daily_slice.rio.to_raster(os.path.join(config['out_dir'], tif_name))

    print(f"   Finished {len(daily_records)} days for {config['name']}")
    return daily_records


# 3. EXECUTION LOOP
all_results = []
for config in study_configs:
    try:
        # Extend the list with the multiple rows returned for each area
        daily_stats = download_and_process(config)
        all_results.extend(daily_stats)
    except Exception as e:
        print(f"!!! Error on {config['name']}: {e}")

# 4. export csv - average radiance per day per study area
if all_results:
    master_df = pd.DataFrame(all_results)
    local_csv = "H:/CEE0189/daily_analysis_summary.csv"
    final_box_csv = "C:/Users/dredhu01/Box/CEE0189/test_output/daily_analysis_summary.csv"

    master_df.to_csv(local_csv, index=False)
    shutil.copy2(local_csv, final_box_csv)
    print(f"\nSUCCESS: CSV with daily histogram shifts saved to {final_box_csv}")