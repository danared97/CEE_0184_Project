import Rbeast as rb
import os
import numpy as np
import pandas as pd
import xarray as xr
import rioxarray
from joblib import Parallel, delayed
import time
from tqdm import tqdm

'''
    {"name": "Cali_prefire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily/California/prefire"},
    {"name": "Cali_fire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily/California/fire"},
    {"name": "Cali_postfire", "out_dir": "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily/California/postfire"},
'''

study_configs = [
    {"name": "argentina_prefire", "out_dir": "H:/backup_step3/daily/Argentina/prefire"},
    {"name": "argentina_fire", "out_dir": "H:/backup_step3/daily/Argentina/fire"},
    {"name": "argentina_postfire", "out_dir": "H:/backup_step3/daily/daily/Argentina/postfire"},
    {"name": "southkorea_prefire", "out_dir": "H:/backup_step3/daily/SouthKorea/prefire"},
    {"name": "southkorea_fire", "out_dir": "H:/backup_step3/daily/SouthKorea/fire"},
    {"name": "southkorea_postfire", "out_dir": "H:/backup_step3/daily/SouthKorea/postfire"},
]

def process_chunk(chunk):
    """Processes a batch of pixels. Threading makes this much more stable on Windows."""
    chunk_results = []
    for ts in chunk:
        ts_clean = ts[~np.isnan(ts)]
        # Skip if not enough data or if the data is a flat line (zero variance)
        if len(ts_clean) < 3 or np.std(ts_clean) == 0:
            chunk_results.append(np.nan)
            continue
        try:
            o = rb.beast(ts_clean, season='none', trend='piecewise', quiet=True)
            chunk_results.append(np.median(o.trend.Y))
        except:
            chunk_results.append(np.nan)
    return chunk_results


def beast_pixel_composite_from_rasters(config, n_jobs=4):
    start_time = time.time()
    print(f"\n{'=' * 60}\n>>> STARTING: {config['name']}\n{'=' * 60}")

    tif_files = sorted([os.path.join(config['out_dir'], f)
                        for f in os.listdir(config['out_dir'])
                        if f.lower().endswith(".tif")])
    if not tif_files:
        print(f"   [!] No rasters found in: {config['out_dir']}")
        return None

    # LOAD & ALIGN
    slices = [rioxarray.open_rasterio(f).isel(band=0).squeeze() for f in tif_files]
    aligned_list = xr.align(*slices, join='outer')
    data_stack = xr.concat(aligned_list, dim='time')

    ntime, nx, ny = data_stack.shape
    # Flatten to (Time, Pixels) -> Transpose to (Pixels, Time)
    flat_data = data_stack.values.reshape(ntime, -1).T

    # FILTER VALID PIXELS (Significant speedup)
    valid_mask = np.count_nonzero(~np.isnan(flat_data), axis=1) >= 3
    data_to_process = flat_data[valid_mask]

    print(f"   Stack: ({ntime}x{nx}x{ny}) | Valid Pixels: {len(data_to_process)} / {nx * ny}")

    # CHUNK WORKLOAD
    chunk_size = 1000
    num_chunks = max(1, len(data_to_process) // chunk_size)
    pixel_chunks = np.array_split(data_to_process, num_chunks)

    # PARALLEL WITH PROGRESS BAR
    # Use backend='threading' for stability and return_as='generator' for tqdm
    print(f"   Launching Parallel BEAST (Threading) on {n_jobs} cores...")

    generator = Parallel(n_jobs=n_jobs, backend="threading", return_as="generator")(
        delayed(process_chunk)(c) for c in pixel_chunks
    )

    # tqdm wraps the generator to track progress by batch
    nested_results = []
    for res in tqdm(generator, total=len(pixel_chunks), desc=f"   {config['name'][:15]}"):
        nested_results.append(res)

    processed_values = [item for sublist in nested_results for item in sublist]

    # RECONSTRUCT GRID
    composite_flat = np.full(nx * ny, np.nan)
    composite_flat[valid_mask] = processed_values
    composite_array = composite_flat.reshape((nx, ny))

    # SAVE & FORMAT
    composite_da = aligned_list[0].copy(data=composite_array)
    out_tif = os.path.join(config['out_dir'], f"{config['name']}_composite_BEAST.tif")
    composite_da.rio.to_raster(out_tif)

    valid_pixels_only = composite_flat[~np.isnan(composite_flat)]
    print(f"   [SAVED] {out_tif}")
    print(f">>> FINISHED {config['name']} in {time.time() - start_time:.2f}s")

    return pd.DataFrame({
        "study_area": config["name"],
        "radiance": valid_pixels_only
    })


if __name__ == "__main__":
    script_start = time.time()
    all_pixel_dfs = []

    for idx, config in enumerate(study_configs):
        print(f"\nProcessing Study Area {idx + 1}/{len(study_configs)}")
        # Adjust n_jobs to your CPU count (4 is safe; try 6-8 if you have 16GB+ RAM)
        df_pixels = beast_pixel_composite_from_rasters(config, n_jobs=4)

        if df_pixels is not None:
            all_pixel_dfs.append(df_pixels)
        time.sleep(1)

    if all_pixel_dfs:
        final_df = pd.concat(all_pixel_dfs, ignore_index=True)
        csv_path = "H:/backup_step3/daily/pixel_composites_BEAST.csv"
        final_df.to_csv(csv_path, index=False)
        print(f"\nSUCCESS: CSV saved to {csv_path}")
        print(f"Total Runtime: {(time.time() - script_start) / 60:.2f} minutes")