import os
import shutil
import numpy as np
import pandas as pd
import geopandas as gpd
import rioxarray
from blackmarble import BlackMarble, Product
from concurrent.futures import ProcessPoolExecutor

study_configs = [
    {"name": "California", "path": "C:/Users/dredhu01/Box/CEE0189/studyareas/cali_2.shp",
     "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly/California", "fire_date": "2025-01-07"},
    {"name": "argentina", "path": "C:/Users/dredhu01/Box/CEE0189/studyareas/argentina_3.shp",
     "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly/Argentina", "fire_date": "2025-01-15"},
    {"name": "southkorea", "path": "C:/Users/dredhu01/Box/CEE0189/studyareas/southkorea.shp",
     "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly/SouthKorea", "fire_date": "2025-03-21"},
]

bm = BlackMarble(
    token="eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImRudmFuaHVpcyIsImV4cCI6MTc3NTkzMjA2MSwiaWF0IjoxNzcwNzQ4MDYxLCJpc3MiOiJodHRwczovL3Vycy5lYXJ0aGRhdGEubmFzYS5nb3YiLCJpZGVudGl0eV9wcm92aWRlciI6ImVkbF9vcHMiLCJhY3IiOiJlZGwiLCJhc3N1cmFuY2VfbGV2ZWwiOjN9.MlsRkLkAEiTovHkM8z2O01LGEnGJozR5cu644CSNh9xZ2o5kUPXNRrdD8-g-X2udn7A9NT48C2ZKc_QICrq0ESfmot7xUSbly-f0VdjBc1go-CNmQgdKOr0pAYvJdrh8FexaMdv2mG0GyBdfQNHIxH5DoHdbpwNjA13CRF0mu_WRlll9_QYLq9iHgRyrqtmX-AG9lwJIfloV7tU-WMf6T_oVGgQKlEwKnxiXoUAl2hEEu1jLrR0cY1OLEuO4M8w6aMdhXndPa4aoSPuSi_KUSc228Wfw8Sb3a75e4RcHpZzZIkL1LWFO0s3G_RDIlCPCNqdpLH7egATIF8pix4GKYA",
    output_directory="C:/Users/dredhu01/Box/CEE0189/output/step3_backup/monthly",
    output_skip_if_exists=True
)


def process_monthly_slice(config, raster, month_str, bin_edges, period_label):
    values = raster.values.flatten()
    values = values[~np.isnan(values)]
    hist, _ = np.histogram(values, bins=bin_edges)

    return {
        "study_area": config["name"],
        "month": month_str,
        "period": period_label,
        "mean_radiance": float(np.mean(values)) if values.size > 0 else np.nan,
        "median_radiance": float(np.median(values)) if values.size > 0 else np.nan,
        "histogram": hist.tolist()
    }


def download_and_process(config):
    print(f"\n>>> Processing: {config['name']}")

    # Define and create folder structure
    subfolders = {
        "prefire": os.path.join(config['out_dir'], "prefire"),
        "fire": os.path.join(config['out_dir'], "fire"),
        "postfire": os.path.join(config['out_dir'], "postfire")
    }
    for folder in subfolders.values():
        os.makedirs(folder, exist_ok=True)

    fire_date = pd.to_datetime(config["fire_date"])
    fire_month = fire_date.to_period('M')

    # Define the three time windows
    periods = {
        "prefire": pd.period_range(start=fire_month - 12, end=fire_month - 1, freq='M'),
        "fire": [fire_month],
        "postfire": pd.period_range(start=fire_month + 1, end=fire_month + 12, freq='M')
    }

    gdf = gpd.read_file(config['path'])
    monthly_records = []
    bin_edges = np.linspace(0, 100, 11)

    for label, months in periods.items():
        save_path = subfolders[label]
        for month in months:
            start_date = month.start_time.strftime('%Y-%m-%d')
            end_date = month.end_time.strftime('%Y-%m-%d')
            print(f"   [{label}] Processing: {month}")

            try:
                with bm.raster(gdf, product_id=Product.VNP46A2, date_range=[start_date, end_date]) as ds:
                    data_in_memory = ds['DNB_BRDF-Corrected_NTL'].load()

                monthly_composite = data_in_memory.median(dim='time')

                # Save to specific subfolder
                out_tif = os.path.join(save_path, f"{config['name']}_{month}.tif")
                monthly_composite.rio.to_raster(out_tif)

                monthly_records.append(
                    process_monthly_slice(config, monthly_composite, str(month), bin_edges, label)
                )
            except Exception as e:
                print(f"   SKIPPING {month}: ({e})")

    return monthly_records


all_results = []
for config in study_configs:
    try:
        monthly_stats = download_and_process(config)
        all_results.extend(monthly_stats)
    except Exception as e:
        print(f"!!! Error on {config['name']}: {e}")

if __name__ == "__main__":
    # 1. Initialize BlackMarble outside the loop if possible
    # 2. Use a ProcessPoolExecutor to run study areas in parallel
    all_results = []

    # max_workers should ideally be the number of study areas or CPU cores (whichever is lower)
    with ProcessPoolExecutor(max_workers=len(study_configs)) as executor:
        # Submit all tasks
        future_to_config = {executor.submit(download_and_process, config): config for config in study_configs}

        for future in future_to_config:
            config = future_to_config[future]
            try:
                monthly_stats = future.result()
                all_results.extend(monthly_stats)
                print(f"--- Completed: {config['name']} ---")
            except Exception as e:
                print(f"!!! Parallel Error on {config['name']}: {e}")

    # Save results
    if all_results:
        master_df = pd.DataFrame(all_results)
        final_box_csv = r"C:\Users\dredhu01\Box\CEE0189\output\monthly_viirs.csv"
        master_df.to_csv(final_box_csv, index=False)
        print(f"\nSUCCESS: CSV saved to {final_box_csv}")