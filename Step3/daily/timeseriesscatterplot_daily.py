import sys
import os
import matplotlib.pyplot as plt

import xarray as xr
import rioxarray
import numpy as np
#import matplotlib.pyplot as plt
import seaborn as sns


def plot_area_comparison(pre_path, post_path, study_name):
    # 1. Load the pre-existing TIFs
    pre_ds = rioxarray.open_rasterio(pre_path)
    post_ds = rioxarray.open_rasterio(post_path)

    # 2. Extract values and flatten to 1D arrays (pixels)
    # We use .values[0] because rioxarray usually loads as (band, y, x)
    pre_vals = pre_ds.values[0].flatten()
    post_vals = post_ds.values[0].flatten()

    # 3. Clean up NaNs and ensure they match 1:1
    # This keeps only pixels that exist in BOTH images
    mask = ~np.isnan(pre_vals) & ~np.isnan(post_vals)
    pre_clean = pre_vals[mask]
    post_clean = post_vals[mask]

    # 4. Plotting
    plt.figure(figsize=(8, 8))

    # Scatterplot
    sns.scatterplot(x=pre_clean, y=post_clean, alpha=0.3, s=10, color='teal', edgecolor=None)

    # 5. Add the 1:1 "Recovery" Line
    # Points below = light decreased | Points on/above = recovered
    max_val = max(pre_clean.max(), post_clean.max()) if pre_clean.size > 0 else 100
    plt.plot([0, max_val], [0, max_val], color='red', linestyle='--', label='1:1 Line (Pre-Fire Levels)')

    # Labels and Style
    plt.title(f"Radiance Comparison: {study_name}")
    plt.xlabel("Pre-Fire Radiance (nW/cm²/sr)")
    plt.ylabel("Post-Fire Radiance (nW/cm²/sr)")
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend()

    # Simple logic check for the user
    plt.text(max_val * 0.05, max_val * 0.9, "Recovered/Increased", color='green', fontsize=10)
    plt.text(max_val * 0.7, max_val * 0.1, "Decreased Light", color='red', fontsize=10)

    plt.show()

# --- To Run ---
# Replace these with your actual local file paths
pre_tif = "C:/Users/dredhu01/Downloads/Cali_postfire_composite_BEAST/Cali_prefire_composite_BEAST.tif"
post_tif = "C:/Users/dredhu01/Downloads/Cali_postfire_composite_BEAST/Cali_postfire_composite_BEAST.tif"

plot_area_comparison(pre_tif, post_tif, "California")
