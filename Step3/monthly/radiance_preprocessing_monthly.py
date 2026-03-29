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
    {"name": "Cali_prefire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly/California/prefire"},
    {"name": "Cali_fire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly/California/fire"},
    {"name": "Cali_postfire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly/California/postfire"},
    {"name": "argentina_prefire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly/Argentina/prefire"},
    {"name": "argentina_fire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly/Argentina/fire"},
    {"name": "argentina_postfire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly/Argentina/postfire"},
    {"name": "southkorea_prefire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly/SouthKorea/prefire"},
    {"name": "southkorea_fire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly/SouthKorea/fire"},
    {"name": "southkorea_postfire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly/SouthKorea/postfire"},
]


def beast_pixel_composite_from_rasters(config, n_jobs=None):
    start_time = time.time()
    print(f"\n{'=' * 60}")
    print(f">>> STARTING: {config['name']}")
    print(f"{'=' * 60}")

    tif_files = sorted([os.path.join(config['out_dir'], f)
                        for f in os.listdir(config['out_dir'])
                        if f.lower().endswith(".tif")])

    if not tif_files:
        print(f"   [!] No rasters found in: {config['out_dir']}")
        return None

    print(f"   Found {len(tif_files)} .tif files. Loading into memory...")

    slices = []
    for f in tif_files:
        da = rioxarray.open_rasterio(f).isel(band=0).squeeze()
        slices.append(da)

    print("   Aligning and stacking rasters...")
    aligned_list = xr.align(*slices, join='outer')
    data_in_memory = xr.concat(aligned_list, dim='time')
    ntime, nx, ny = data_in_memory.shape
    print(f"   Stack Shape: (Time: {ntime}, Width: {nx}, Height: {ny})")
    print(f"   Total Pixels to process: {nx * ny}")

    def compute_pixel(i, j):
        ts = data_in_memory[:, i, j].values
        ts = ts[~np.isnan(ts)]
        if len(ts) < 3:
            return np.nan
        try:
            o = rb.beast(ts, season='harmonic', period=12, deltat=1/12, quiet=True)
            return np.median(o.trend.Y)
        except:
            return np.nan

    n_jobs = n_jobs or 4
    print(f"   Launching Parallel BEAST on {n_jobs} cores...")
    pixel_indices = [(i, j) for i in range(nx) for j in range(ny)]

    calc_start = time.time()
    results = Parallel(n_jobs=n_jobs, verbose=5)(delayed(compute_pixel)(i, j) for i, j in pixel_indices)
    calc_end = time.time()

    print(f"   Calculation complete. Time taken for pixels: {calc_end - calc_start:.2f}s")

    composite_array = np.array(results).reshape((nx, ny))

    print("   Formatting output raster...")
    composite_da = data_in_memory.isel(time=0).copy()
    composite_da.values = composite_array
    out_tif = os.path.join(config['out_dir'], f"{config['name']}_composite_BEAST.tif")

    est_size_gb = (nx * ny * 8) / (1024 ** 3)
    print(f"   Estimated file size: {est_size_gb:.4f} GB")

    composite_da.rio.to_raster(out_tif)
    print(f"   [SAVED] Raster: {out_tif}")

    pixels = composite_array.flatten()
    valid_pixels = pixels[~np.isnan(pixels)]
    print(f"   Valid non-NaN pixels extracted: {len(valid_pixels)}")

    total_time = time.time() - start_time
    print(f">>> FINISHED {config['name']} in {total_time:.2f}s\n")

    return pd.DataFrame({
        "study_area": config["name"],
        "radiance": valid_pixels
    })


if __name__ == "__main__":
    script_start = time.time()
    all_pixel_dfs = []

    for idx, config in enumerate(study_configs):
        print(f"\nProcessing Study Area {idx + 1}/{len(study_configs)}")
        df_pixels = beast_pixel_composite_from_rasters(config, n_jobs=4)

        if df_pixels is not None:
            all_pixel_dfs.append(df_pixels)

        print("   Pausing 2 seconds for Box Drive sync...")
        time.sleep(2)

    if all_pixel_dfs:
        print("\nMerging all results into final DataFrame...")
        final_df = pd.concat(all_pixel_dfs, ignore_index=True)
        csv_path = "C:/Users/dredhu01/Box/CEE0189/output/Step3/pixel_composites_BEAST_from_local.csv"
        final_df.to_csv(csv_path, index=False)
        print(f"\n{'*' * 40}")
        print(f" SUCCESS: Final CSV saved to {csv_path}")
        print(f" Total Script Runtime: {(time.time() - script_start) / 60:.2f} minutes")
        print(f"{'*' * 40}")
