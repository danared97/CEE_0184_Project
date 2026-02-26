#import raster worldpop data as a raster array
#finer-resolution data for the populations considered to be higher concern - south korea over 65, argentina under 20

#we also have finer-resolution california data in a shapefile (California NRI and cal_demographics, saved here: https://drive.google.com/file/d/1_Q1GzXPh6rnh3IQDzpZ7rFto9tBqn_z0/view?usp=drive_link
#this could probably be incorporated just with geopandas
#incorporate columns for cal_demographics:
# - median_house_income
# - population above 65
# - populatio below 18

#incorporate columns for cal_NRI:
# - WFIR_RISKR, 'Wildfire - Hazard Type Risk Index Score' (range from very low to very high)
# - SOVI_SPCTL, 'Social Vulnerability - State Percentile'
# - RESL_SPCTL, 'Community Resilience - State Percentile'

#southkorea tifs link: https://drive.google.com/file/d/1nBf3dLpHBulVz1sxVnBVhigz2Fz1ujGp/view?usp=drive_link
#argentina tifs link: https://drive.google.com/file/d/1zWhd7kH9fyG5Dd-3UHqgNZL0e5sGPvtM/view?usp=drive_link

import rasterio
import glob
import numpy as np

def process_worldpop_rasters(file_paths):
    data_layers = {}
    combined_array = None

    for fp in file_paths:
        with rasterio.open(fp) as src:
            # Read first band and set 'no data' values to 0 for math
            data = src.read(1)
            data[data < 0] = 0

            # Store by filename for individual demographic access
            layer_name = fp.split('/')[-1]
            data_layers[layer_name] = data

            # Create a running sum (e.g., to get total pop from cohorts)
            if combined_array is None:
                combined_array = np.zeros_like(data)
            combined_array += data

    return data_layers, combined_array


# Example Usage: Grab all cohort files in a folder
# Use the glob module to find files: https://docs.python.org
southkorea_files = glob.glob("C:/Users/dredhu01/Box/CEE0189/Drive_data/southkorea/*.tif")
argentina_files = glob.glob("C:/Users/dredhu01/Box/CEE0189/Drive_data/argentina/*.tif")

layers, total_pop = process_worldpop_rasters(southkorea_files)
layers, total_pop = process_worldpop_rasters(argentina_files)


print(f"Processed {len(layers)} demographic layers.")

