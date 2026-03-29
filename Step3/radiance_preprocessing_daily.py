import Rbeast as rb
import os
import numpy as np
import pandas as pd
import xarray as xr
import rioxarray
from joblib import Parallel, delayed
import multiprocessing
import time


study_configs = [
    {"name": "Cali_prefire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily/California/prefire"},
    {"name": "Cali_fire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily/California/fire"},
    {"name": "Cali_postfire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily/California/postfire"},
    {"name": "argentina_prefire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily/Argentina/prefire"},
    {"name": "argentina_fire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily/Argentina/fire"},
    {"name": "argentina_postfire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily/Argentina/postfire"},
    {"name": "southkorea_prefire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily/SouthKorea/prefire"},
    {"name": "southkorea_fire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily/SouthKorea/fire"},
    {"name": "southkorea_postfire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily/SouthKorea/postfire"},
]


def beast_pixel_composite_from_rasters(config, n_jobs=None):
    print(f"\n>>> Processing: {config['name']}")

    tif_files = sorted([os.path.join(config['out_dir'], f)
                        for f in os.listdir(config['out_dir'])
                        if f.lower().endswith(".tif")])

    if not tif_files:
        print("   No rasters found in folder!")
        return None

    # Load and clean slices
    slices = []
    for f in tif_files:
        da = rioxarray.open_rasterio(f).isel(band=0).squeeze()
        slices.append(da)

    # Robust Alignment & Stacking
    aligned_list = xr.align(*slices, join='outer')
    data_in_memory = xr.concat(aligned_list, dim='time')
    ntime, nx, ny = data_in_memory.shape

    # Inner function for parallel compute
    def compute_pixel(i, j):
        ts = data_in_memory[:, i, j].values
        ts = ts[~np.isnan(ts)]
        if len(ts) < 3:
            return np.nan
        try:
            o = rb.beast(ts, season='none', trend='piecewise', quiet=True)
            return np.median(o.trend.Y)
        except:
            return np.nan

    # Parallel Execution
    n_jobs = n_jobs or 4
    print(f"   Running BEAST on {n_jobs} cores...")
    pixel_indices = [(i, j) for i in range(nx) for j in range(ny)]
    results = Parallel(n_jobs=n_jobs)(delayed(compute_pixel)(i, j) for i, j in pixel_indices)

    # Reshape results directly into the grid
    composite_array = np.array(results).reshape((nx, ny))

    # Save the resulting Raster
    composite_da = data_in_memory.isel(time=0).copy()
    composite_da.values = composite_array
    out_tif = os.path.join(config['out_dir'], f"{config['name']}_composite_BEAST.tif")

    total_pixels = nx * ny
    bytes_per_pixel = 8  # Standard for float64 decimal data

    # Calculate in GB
    est_size_gb = (total_pixels * bytes_per_pixel) / (1024 ** 3)

    print(f"   Estimated file size: {est_size_gb:.4f} GB")

    composite_da.rio.to_raster(out_tif)
    print(f"   Saved raster: {out_tif}")

    # Create data for the CSV
    pixels = composite_array.flatten()
    pixels = pixels[~np.isnan(pixels)]
    return pd.DataFrame({
        "study_area": config["name"],
        "radiance": pixels
    })


if __name__ == "__main__":
    all_pixel_dfs = []

    for config in study_configs:
        df_pixels = beast_pixel_composite_from_rasters(config, n_jobs=4)
        if df_pixels is not None:
            all_pixel_dfs.append(df_pixels)

        # Pause to let Box Drive sync without crashing
        print("   Pausing 2 seconds for Box sync...")
        time.sleep(2)

    if all_pixel_dfs:
        final_df = pd.concat(all_pixel_dfs, ignore_index=True)
        csv_path = "C:/Users/dredhu01/Box/CEE0189/output/Step3/pixel_composites_BEAST_from_local.csv"
        final_df.to_csv(csv_path, index=False)
        print(f"\n SUCCESS: All data saved to {csv_path}")
