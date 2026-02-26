#import raster worldpop data as a raster array

#finer-resolution data for the populations considered to be higher concern - south korea over 65, argentina under 20

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

