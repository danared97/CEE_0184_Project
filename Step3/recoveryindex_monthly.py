import xarray as xr
import os
import pandas as pd
import rasterio

regions = ["California", "Argentina", "SouthKorea"]
base_path = "C:/Users/dredhu01/Box/CEE0189/output/Step3/monthly"

for reg in regions:
    # 1. Load the two static 'baseline' rasters
    pre_path = f"{base_path}/{reg}/prefire/{reg}_prefire_composite_BEAST.tif"
    imm_post_path = f"{base_path}/{reg}/fire/{reg}_fire_composite_BEAST.tif"

    ntl_pre = xr.open_rasterio(pre_path).squeeze()
    ntl_imm_post = xr.open_rasterio(imm_post_path).squeeze()

    # 2. Load the 24 monthly post-fire rasters into a stack
    # Assumes files are named something like 'Cali_postfire_month_01.tif'
    post_dir = f"{base_path}/{reg}/postfire/"
    monthly_files = sorted([os.path.join(post_dir, f) for f in os.listdir(post_dir) if "month" in f])

    # Create a time-indexed stack
    monthly_series = xr.concat([xr.open_rasterio(f).squeeze() for f in monthly_files], dim="time")
    monthly_series['time'] = pd.date_range(start='2024-01-01', periods=24, freq='MS')  # Adjust start date

    # 3. Vectorized Calculation (Equation applied to all 24 months simultaneously)
    denom = ntl_pre - ntl_imm_post
    denom = denom.where(denom != 0)  # Safety for divide-by-zero

    recovery_stack = (monthly_series - ntl_imm_post) / denom

    # 4. Save as a multi-band NetCDF or individual monthly TIFs
    out_nc = f"{base_path}/{reg}/{reg}_24Month_Recovery_Trend.nc"
    recovery_stack.to_netcdf(out_nc)
    print(f"Saved 24-month recovery trend for {reg}")
