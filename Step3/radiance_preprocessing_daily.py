import os
import numpy as np
import pandas as pd
import xarray as xr
import rioxarray
import Rbeast as rb
from joblib import Parallel, delayed
import multiprocessing
import matplotlib.pyplot as plt

# 1. Study areas & folders
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

# 2. BEAST pixel-level composite from existing rasters

def beast_pixel_composite_from_rasters(config, n_jobs=None):
    print(f"\n>>> Processing: {config['name']}")

    # list all TIFs in out_dir
    tif_files = sorted([os.path.join(config['out_dir'], f)
                        for f in os.listdir(config['out_dir'])
                        if f.lower().endswith(".tif")])

    if len(tif_files) == 0:
        print("   No rasters found in folder!")
        return None

    print(f"   Found {len(tif_files)} daily rasters")
    slices = [rioxarray.open_rasterio(f).squeeze() for f in tif_files]

    # stack into 3D array: (time, x, y)
    data_in_memory = xr.concat(slices, dim='time')
    ntime, nx, ny = data_in_memory.shape
    composite_array = np.full((nx, ny), np.nan)

    # function to compute BEAST for a single pixel
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

    # number of cores
    if n_jobs is None:
        n_jobs = max(1, multiprocessing.cpu_count() - 1)

    print(f"   Running BEAST in parallel using {n_jobs} cores...")
    pixel_indices = [(i, j) for i in range(nx) for j in range(ny)]
    from joblib import Parallel, delayed
    results = Parallel(n_jobs=n_jobs)(delayed(compute_pixel)(i, j) for i, j in pixel_indices)

    # reshape to 2D: takes a collection of data, converts it to a numpy array, rearranges that data into a 2d structure with nx rows and ny
    # columns, then copies that reshaped data into a 2d structure
    composite_array[:, :] = np.array(results).reshape((nx, ny))

    # save raster
    composite_da = data_in_memory.isel(time=0).copy()
    composite_da.values = composite_array
    out_tif = os.path.join(config['out_dir'], f"{config['name']}_composite_BEAST.tif")
    composite_da.rio.to_raster(out_tif)
    print(f"   Saved BEAST-based composite to {out_tif}")

    # create pixel-level dataframe
    pixels = composite_array.flatten()
    pixels = pixels[~np.isnan(pixels)]
    df_pixels = pd.DataFrame({
        "study_area": config["name"],
        "period": config["name"].split("_")[-1],  # prefire/fire/postfire
        "radiance": pixels
    })

    return df_pixels

# 3. Run for all study areas

all_pixel_dfs = []
for config in study_configs:
    df_pixels = beast_pixel_composite_from_rasters(config, n_jobs=8)
    if df_pixels is not None:
        all_pixel_dfs.append(df_pixels)

# combine all pixel-level data
if all_pixel_dfs:
    pixel_data = pd.concat(all_pixel_dfs, ignore_index=True)
    pixel_csv = "C:/Users/dredhu01/Box/CEE0189/output/Step3/pixel_composites_BEAST_from_local.csv"
    pixel_data.to_csv(pixel_csv, index=False)
    print(f"\n Pixel-level data saved to {pixel_csv}")

# 4. Example scatter plot

area = "Cali"
pre_pixels = pixel_data[pixel_data.study_area == f"{area}_prefire"]["radiance"].values
post_pixels = pixel_data[pixel_data.study_area == f"{area}_postfire"]["radiance"].values

plt.figure(figsize=(6, 6))
plt.scatter(pre_pixels, post_pixels, alpha=0.3)
plt.xlabel("Pre-fire radiance")
plt.ylabel("Post-fire radiance")
plt.title(f"Pixel-level VIIRS NTL recovery: {area}")
plt.plot([0, max(pre_pixels.max(), post_pixels.max())],
         [0, max(pre_pixels.max(), post_pixels.max())], 'r--')
plt.show()