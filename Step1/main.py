import os
import time
import blackmarble
import numpy as np
import shutil
import pandas as pd
import geopandas as gpd
import rioxarray

from blackmarble import BlackMarble, Product


# define region of interest
study_configs = [
    {"name": "Cali", "path": "...", "out_dir": "...", "start": "2025-01-01", "end": "2025-02-11"},
    {"name": "Argentina", "path": "...", "out_dir": "...", "start": "2025-01-01", "end": "2025-03-02"},
    {"name": "SouthKorea", "path": "...", "out_dir": "...", "start": "2025-03-14", "end": "2025-04-03"},
]

#set up blackmarble client - need to have an Earthdata access token that allows clearance to LAADS DAAC
bm = BlackMarble(
    token="...",
    output_directory="...", # Choose any local(!!!) folder
    output_skip_if_exists=True
)

# Function: runs through each study area and returns a daily mosaicked TIF output for full area
#Gemini was utilized for lines 63-90 in order to allow for manual image selection when dates were not available - the code
#runs through the timeline provided, determines if imagery is available for all dates. If it is, then the manifest order
#is sent to the DAAC. If not, orders are sent individually with missing dates skipped.

# Ensure "out_dir" is a unique subfolder for each area inside your Box path
base_box_path = "..."

def download_and_process(config):
    print(f"\n>>> Processing: {config['name']}")
    if not os.path.exists(config['out_dir']): os.makedirs(config['out_dir'])

    dates = pd.date_range(start=config['start'], end=config['end']).strftime('%Y-%m-%d').tolist()
    gdf = gpd.read_file(config['path'])
    daily_records = []
    bin_edges = np.linspace(0, 100, 11)

    # Attempt full date range
    try:
        print(f"   Attempting bulk download for {len(dates)} days...")
        with bm.raster(gdf, product_id=Product.VNP46A2, date_range=dates) as ds:
            data_in_memory = ds['DNB_BRDF-Corrected_NTL'].load()

        # If successful, process all time slices in RAM
        for i, date_val in enumerate(data_in_memory.time.values):
            date_str = pd.to_datetime(date_val).strftime('%Y-%m-%d')
            daily_slice = data_in_memory.isel(time=i)
            daily_records.append(process_daily_slice(config, daily_slice, date_str, bin_edges))

    # individual days (slow)
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

# execute function
all_results = []
for config in study_configs:
    try:
        # Extend the list with the multiple rows returned for each area
        daily_stats = download_and_process(config)
        all_results.extend(daily_stats)
    except Exception as e:
        print(f"!!! Error on {config['name']}: {e}")

# 4. export csv (Will have one row per day per area, average radiance per day)
if all_results:
    master_df = pd.DataFrame(all_results)
    local_csv = "..."
    #was having issues saving to box, this just saves locally and then to box
    final_box_csv = "..."

    master_df.to_csv(local_csv, index=False)
    shutil.copy2(local_csv, final_box_csv)
    print(f"\nSUCCESS: CSV with daily histogram shifts saved to {final_box_csv}")