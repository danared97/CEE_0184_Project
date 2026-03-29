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
    start_total = time.time()
    print(f"\n>>> Starting: {config['name']}")

    # 1. FIND FILES
    tif_files = sorted([os.path.join(config['out_dir'], f)
                        for f in os.listdir(config['out_dir'])
                        if f.lower().endswith(".tif")])

    if not tif_files:
        print("   [!] No rasters found!")
        return None

    # 2. LOAD & ALIGN
    start_load = time.time()
    slices = []
    for f in tif_files:
        # Open and ensure we keep the spatial coords
        da = rioxarray.open_rasterio(f).isel(band=0).squeeze()
        slices.append(da)

    # Aligning ensures all rasters share the exact same grid
    aligned_list = xr.align(*slices, join='outer')
    data_stack = xr.concat(aligned_list, dim='time')

    # Identify dimensions clearly: rioxarray usually provides (time, y, x)
    # y = Rows (Lat), x = Cols (Lon)
    ntime, ny, nx = data_stack.shape
    print(f"   [Data Loaded] Shape: {ntime} times, {ny} rows, {nx} cols")
    print(f"   [Time] Loading/Aligning took: {time.time() - start_load:.2f}s")

    # 3. COMPUTE PIXELS
    def compute_pixel(y_idx, x_idx):
        ts = data_stack[:, y_idx, x_idx].values
        ts = ts[~np.isnan(ts)]
        if len(ts) < 3:
            return np.nan
        try:
            o = rb.beast(ts, season='none', trend='piecewise', quiet=True)
            return np.median(o.trend.Y)
        except:
            return np.nan

    n_jobs = n_jobs or 4
    print(f"   [Processing] Running BEAST on {n_jobs} cores...")
    start_proc = time.time()

    # Create coordinate pairs (y, x) to match array indexing
    pixel_indices = [(y, x) for y in range(ny) for x in range(nx)]
    results = Parallel(n_jobs=n_jobs)(delayed(compute_pixel)(y, x) for y, x in pixel_indices)

    proc_duration = time.time() - start_proc
    print(f"   [Time] BEAST processing took: {proc_duration:.2f}s ({(proc_duration / 60):.2f} mins)")

    # 4. RECONSTRUCT RASTER
    # Map results back to 2D grid
    composite_array = np.array(results).reshape((ny, nx))

    # Copy the metadata from the first slice of the stack
    composite_da = data_stack.isel(time=0).copy(deep=True)
    composite_da.values = composite_array

    # Ensure spatial metadata is preserved
    if hasattr(data_stack, 'rio'):
        composite_da.rio.write_crs(data_stack.rio.crs, inplace=True)
        composite_da.rio.write_transform(data_stack.rio.transform(), inplace=True)

    # 5. SAVE
    start_save = time.time()
    out_tif = os.path.join(config['out_dir'], f"{config['name']}_composite_BEAST.tif")
    composite_da.rio.to_raster(out_tif)

    print(f"   [Saved] Raster: {out_tif}")
    print(f"   [Time] Saving took: {time.time() - start_save:.2f}s")
    print(f"   >>> Finished {config['name']} in {time.time() - start_total:.2f}s")

    # Return CSV data
    pixels = composite_array.flatten()
    pixels = pixels[~np.isnan(pixels)]
    return pd.DataFrame({"study_area": config["name"], "radiance": pixels})

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
