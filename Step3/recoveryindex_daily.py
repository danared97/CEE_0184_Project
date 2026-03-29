import xarray as xr
import os
import numpy as np
import rasterio

# Mapping your directory names to the formula terms
regions = ["California", "Argentina", "SouthKorea"]
base_path = "C:/Users/dredhu01/Box/CEE0189/output/Step3/daily"

for reg in regions:
    print(f"\n--- Calculating Recovery Index for: {reg} ---")

    # NTL_pre-fire
    pre_path = f"{base_path}/{reg}/prefire/{reg.lower() if reg != 'California' else 'Cali'}_prefire_composite_BEAST.tif"
    # NTL_immediate post-fire (The "fire" period output)
    fire_path = f"{base_path}/{reg}/fire/{reg.lower() if reg != 'California' else 'Cali'}_fire_composite_BEAST.tif"
    # NTL_t (The "post-fire" period output)
    post_path = f"{base_path}/{reg}/postfire/{reg.lower() if reg != 'California' else 'Cali'}_postfire_composite_BEAST.tif"

    try:
        ntl_pre = xr.open_rasterio(pre_path).squeeze()
        ntl_imm_post = xr.open_rasterio(fire_path).squeeze()
        ntl_t = xr.open_rasterio(post_path).squeeze()

        # Numerator: NTL_t - NTL_immediate_post-fire
        numerator = ntl_t - ntl_imm_post

        # Denominator: NTL_pre-fire - NTL_immediate_post-fire
        denominator = ntl_pre - ntl_imm_post

        # Avoid division by zero for pixels with no change
        denominator = denominator.where(denominator != 0)

        # Final Recovery Index Calculation
        recovery_index = numerator / denominator

        # Save result
        out_tif = f"{base_path}/{reg}/{reg}_Recovery_Index.tif"
        recovery_index.rio.to_raster(out_tif)
        print(f"   [SUCCESS] Recovery Index saved: {out_tif}")

    except Exception as e:
        print(f"   [ERROR] Failed to process {reg}: {e}")
