import rasterio
from rasterio import features
import geopandas as gpd
import numpy as np
import glob
import os
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix, classification_report

# --- 1. CONFIGURATION & PATHS ---
data_path = r"C:\Users\dredhu01\Box\CEE0189\test_output\boston_snowstorm\VNP46A1"
shape_path = os.path.join(data_path, "outage_area.shp")  # Your Cape Cod shapefile
all_tifs = sorted(glob.glob(os.path.join(data_path, "MA_*.tif")))

# Timeline Definitions (11 Days)
pre_files = all_tifs[0:4]  # Days 1-4 (Baseline)
post_files = all_tifs[5:11]  # Days 6-11 (Analysis Window)
day_6_file = all_tifs  # Focus day for Confusion Matrix


# --- 2. THE CORE PROCESSING FUNCTION (WITH MOONLIGHT CORRECTION) ---
def load_and_correct(f_path):
    """Loads TIFF and subtracts the 5th percentile 'Moonlight/Sky' floor."""
    with rasterio.open(f_path) as src:
        data = src.read(1).astype('float32')
        data[data == 65535] = np.nan

        # MOONLIGHT POST-PROCESSING:
        # We find the 5th percentile (the darkest pixels, likely ocean/forest)
        # and subtract that 'floor' from the entire image to normalize for moonlight.
        moonlight_floor = np.nanpercentile(data, 5)
        corrected_data = data - moonlight_floor
        corrected_data[corrected_data < 0] = 0  # Radiance cannot be negative

        return corrected_data, src.profile, src.crs, src.transform


# --- 3. CALCULATE BASELINE & RASTERIZE SHAPEFILE ---
print("Step 1: Calculating Pre-Disaster Baseline and Fluctuations...")
pre_stack = []
for f in pre_files:
    arr, meta, crs, transform = load_and_correct(f)
    pre_stack.append(arr)

pre_mean = np.nanmean(pre_stack, axis=0)
pre_std = np.nanstd(pre_stack, axis=0)

print("Step 2: Reprojecting Shapefile and Creating Ground Truth Mask...")
gdf = gpd.read_file(shape_path).to_crs(crs)  # MATCH PROJECTION
ground_truth_mask = features.rasterize(
    [(shape, 1) for shape in gdf.geometry],
    out_shape=pre_mean.shape, transform=transform, fill=0, all_touched=True, dtype='uint8'
)

# --- 4. ANALYZE DAY 6 & GENERATE CONFUSION MATRIX ---
print("Step 3: Validating Day 6 Outages on Cape Cod...")
day_6_rad, _, _, _ = load_and_correct(day_6_file)

# Logic: Outage = (Day_6 < Pre_Mean - 2 * Pre_Std)
# This uses your 'Pre-Disaster Daily NTL Fluctuations' requirement
outage_pred = np.where(day_6_rad < (pre_mean - 2 * pre_std), 1, 0)
outage_pred[pre_mean < 1.5] = 0  # Ignore naturally dark noise-floor pixels

# Filter data for Confusion Matrix (Only pixels with baseline radiance > 1.5)
valid_pixels = (pre_mean > 1.5)
y_true = ground_truth_mask[valid_pixels].flatten()
y_pred = outage_pred[valid_pixels].flatten()

print("\n" + "=" * 40)
print("CONFUSION MATRIX: CAPE COD (DAY 6)")
print("=" * 40)
print(confusion_matrix(y_true, y_pred))
print("\nClassification Report:")
print(classification_report(y_true, y_pred, target_names=['No Outage', 'Outage']))

# --- 5. CALCULATE 11-DAY RECOVERY TREND ---
print("\nStep 4: Calculating 11-Day Recovery Trendline...")
daily_means_cape = []
for f in all_tifs:
    data, _, _, _ = load_and_correct(f)
    # Average radiance inside your Cape shapefile area
    daily_means_cape.append(np.nanmean(data[ground_truth_mask == 1]))

# Normalize to Percent of Normal (Days 1-4 Mean = 100%)
baseline_val = np.mean(daily_means_cape[0:4])
percent_recovery = [(v / baseline_val) * 100 for v in daily_means_cape]

# --- 6. VISUALIZATION ---
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Subplot A: Scatterplot (Radiance Points)
mask = (pre_mean > 1.5) & (pre_mean < 60)
x_pts = pre_mean[mask].flatten()
y_pts = day_6_rad[mask].flatten()
idx = np.random.choice(len(x_pts), 5000)  # Sample for speed

ax1.scatter(x_pts[idx], y_pts[idx], alpha=0.2, s=2, color='gray', label='All Pixels')
# Highlight Cape Cod pixels in red
cape_mask = (ground_truth_mask == 1) & (pre_mean > 1.5)
ax1.scatter(pre_mean[cape_mask], day_6_rad[cape_mask], color='red', s=5, alpha=0.5, label='Cape Cod (Outage Area)')
ax1.plot(,, 'k--', label = '1:1 Normal Line')
ax1.set_title('Radiance Comparison: Pre-Mean vs Day 6 (Moon-Corrected)')
ax1.set_xlabel('Pre-Disaster Radiance ($nW/cm^2/sr$)')
ax1.set_ylabel('Day 6 Radiance ($nW/cm^2/sr$)')
ax1.legend()

# Subplot B: Recovery Trendline
days = list(range(1, 12))
ax2.plot(days, percent_recovery, marker='o', color='red', linewidth=2, label='Cape Cod Mean')
ax2.axhline(100, color='black', linestyle='--', alpha=0.5, label='100% Baseline')
ax2.axvspan(4.5, 5.5, color='blue', alpha=0.1, label='Snowstorm (Day 5)')
ax2.set_title('11-Day NTL Recovery Trend')
ax2.set_xlabel('Day')
ax2.set_ylabel('Percent of Normal Radiance (%)')
ax2.set_xticks(days)
ax2.legend()

plt.tight_layout()
plt.show()

# --- 7. EXPORT PERCENT NORMAL GEOTIFF ---
meta.update(dtype='float32', count=1)
with np.errstate(divide='ignore', invalid='ignore'):
    pn_map = 100 * (day_6_rad / pre_mean)
    pn_map = np.nan_to_num(pn_map, nan=0)

with rasterio.open('Boston_PercentNormal_Day6.tif', 'w', **meta) as dst:
    dst.write(pn_map.astype('float32'), 1)

print("\nProcess Complete. File saved: Boston_PercentNormal_Day6.tif")
