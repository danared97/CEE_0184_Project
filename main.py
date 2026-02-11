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
    #{"name": "Cali", "path": "C:/Users/dredhu01/Box/CEE0189/studyareas/cali_test.shp", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/California", "start": "2025-01-01", "end": "2025-02-11"},
    {"name": "Argentina", "path": "C:/Users/dredhu01/Box/CEE0189/studyareas/argentina.shp", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Argentina", "start": "2025-01-01", "end": "2025-03-02"},
    #{"name": "SouthKorea", "path": "C:/Users/dredhu01/Box/CEE0189/studyareas/southkorea.shp", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/SouthKorea", "start": "2025-03-14", "end": "2025-04-03"},
]

# ------------------------------------------------------------------------------
# 2. Set up the BlackMarble client
# ------------------------------------------------------------------------------
# If the environment variable `BLACKMARBLE_TOKEN` is set, it will be used automatically.
# You can also pass your token directly, but using the environment variable is recommended.
bm = BlackMarble(
    token="...",
    output_directory="...", # Choose any local folder on your C: drive
    output_skip_if_exists=True
)
# ------------------------------------------------------------------------------
# 3. Download VNP46 data from NASA Earthdata
# ------------------------------------------------------------------------------
# Function runs through each study area and returns a daily mosaicked TIF output for full area
#Gemini was utilized for lines 63-90 in order to allow for manual image selection when dates were not available - the code
#runs through the timeline provided, determines if imagery is available for all dates. If it is, then the manifest order
#is sent to the DAAC. If not, orders are sent individually with missing dates skipped.
# Use this name consistently
# Ensure "out_dir" is a unique subfolder for each area inside your Box path
base_box_path = "..."

# 3. THE PROCESSING FUNCTION
def download_and_process(config):
    print(f"\n>>> Processing: {config['name']}")
    if not os.path.exists(config['out_dir']): os.makedirs(config['out_dir'])

    dates = pd.date_range(start=config['start'], end=config['end']).strftime('%Y-%m-%d').tolist()
    gdf = gpd.read_file(config['path'])
    daily_records = []
    bin_edges = np.linspace(0, 100, 11)

    # --- METHOD 1: Attempt Whole Range (Fastest) ---
    try:
        print(f"   Attempting bulk download for {len(dates)} days...")
        with bm.raster(gdf, product_id=Product.VNP46A2, date_range=dates) as ds:
            data_in_memory = ds['DNB_BRDF-Corrected_NTL'].load()

        # If successful, process all time slices in RAM
        for i, date_val in enumerate(data_in_memory.time.values):
            date_str = pd.to_datetime(date_val).strftime('%Y-%m-%d')
            daily_slice = data_in_memory.isel(time=i)
            daily_records.append(process_daily_slice(config, daily_slice, date_str, bin_edges))

    # --- METHOD 2: Fallback to Individual Days (Reliable) ---
    except Exception as e:
        if "manifest" in str(e) or "required files could not found" in str(e):
            print(f"   Manifest error detected. Switching to individual day collection...")
            for date_str in dates:
                try:
                    with bm.raster(gdf, product_id=Product.VNP46A2, date_range=date_str) as ds:
                        daily_slice = ds['DNB_BRDF-Corrected_NTL'].load().isel(time=0)
                    daily_records.append(process_daily_slice(config, daily_slice, date_str, bin_edges))
                    print(f"   Successfully collected: {date_str}")
                except Exception as day_error:
                    print(f"   SKIPPING {date_str}: Data truly missing ({day_error})")
        else:
            raise e # Re-raise if it's a different error (like path or token issues)

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

# 4. EXPORT MASTER CSV (Will have one row per day per area)
if all_results:
    master_df = pd.DataFrame(all_results)
    local_csv = "H:/CEE0189/daily_analysis_summary.csv"
    final_box_csv = "C:/Users/dredhu01/Box/CEE0189/test_output/daily_analysis_summary.csv"

    master_df.to_csv(local_csv, index=False)
    shutil.copy2(local_csv, final_box_csv)
    print(f"\nSUCCESS: CSV with daily histogram shifts saved to {final_box_csv}")