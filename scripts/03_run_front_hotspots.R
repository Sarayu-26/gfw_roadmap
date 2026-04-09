# =============================================================================
# Generate ocean front hotspot polygons from raster products
#
# Author: Isaac Brito-Morales
# Email: ibrito@conservation.org
# =============================================================================

# --- Project runtime (safe to call repeatedly)
source("renv/activate.R")

source("R/load_packages.R")
source("R/utils_helpers.R")

# --- Load hotspot function
source("R/front_hotspot_polygons.R")


# =============================================================================
# 1. FSLE FRONT HOTSPOTS (absolute threshold - original workflow)
# =============================================================================

make_front_polygon(
  rs_dir = "outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q3_pct.tif",
  cutoff = 50,
  cutoff_type = "absolute",
  outdir = "outputs/fsle_front_polygons",
  outfile = "fsle_quartiles_1994_2022_Q3_pct_cut50"
)


# =============================================================================
# 2. THERMAL FRONT HOTSPOTS (top decile / top quartile)
# =============================================================================

make_front_polygon(
  rs_dir = "data/fronts_thermal/global_thermal_fronts_climatology_miller_v1.0_overall.g7.front_step4_sst.UIR.L3_pfront.data.nc",
  cutoff = 0.75,
  cutoff_type = "quantile",
  outdir = "outputs/thermal_front_polygons",
  outfile = "thermal_front_persistence_q90_minpatch20",
  remove_small_patches = TRUE,
  min_patch_cells = 20
)

make_front_polygon(
  rs_dir = "data/fronts_thermal/global_thermal_fronts_climatology_miller_v1.0_overall.g7.front_step4_sst.UIR.L3_pfront.data_median39.nc",
  cutoff = 0.75,
  cutoff_type = "quantile",
  outdir = "outputs/thermal_front_polygons",
  outfile = "thermal_front_persistence_q75_data_median39_minpatch70",
  remove_small_patches = TRUE,
  min_patch_cells = 70
)


# =============================================================================
# 3. FSLE HOTSPOTS USING QUANTILE APPROACH (optional comparison)
# =============================================================================

make_front_polygon(
  rs_dir = "outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q3_pct.tif",
  cutoff = 0.9,
  cutoff_type = "quantile",
  outdir = "outputs/fsle_front_polygons",
  outfile = "fsle_quartiles_1994_2022_Q3_pct_q90_minpatch70",
  remove_small_patches = TRUE,
  min_patch_cells = 70
)
