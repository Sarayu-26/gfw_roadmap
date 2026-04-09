# ==============================================================================
#  This code was created by Isaac Brito-Morales
#  (ibrito@conservation.org)
# ==============================================================================

# ==============================================================================
# Area statistics: inside vs outside front polygons, globally and by latitude band
#
# Purpose:
#   - Compute total area inside (FSLE OR thermal)
#   - Compute area with species present (rs >= 1) inside
#   - Compute total area outside
#   - Compute area with species present outside
#   - Derive unweighted proportions
#   - Compute richness-weighted area inside and outside
#   - Repeat all metrics globally and by latitude band
#
# Expected input:
#   outputs/count_per_pixel_birdlife_plus_sdms.tif
#   outputs/fsle_front_polygons/fsle_quartiles_1994_2022_Q3_pct_cut50.rds
#   outputs/thermal_front_polygons/thermal_front_persistence_q75_data_median39.rds
#
# Creates:
#   (optional)
#   outputs/area_stats_inside_outside_by_lat_band.csv
#
# Notes:
# - Uses raster grid (no polygon union)
# - "inside" = union of FSLE and thermal front polygons
# - "with data" = rs >= 1 (presence of at least one species)
# - Unweighted metrics represent occupied area
# - Richness-weighted metrics use cell area * species count per cell
# - Latitude bands include:
#     global
#     tropics, tropics_north, tropics_south
#     temperate, temperate_north, temperate_south
#     polar, polar_north, polar_south
# - Cell area is computed in km2 using terra::cellSize
# - Latitude bands are restricted to the same ocean-only domain as the original code
# ==============================================================================

# --- Setup
source("renv/activate.R")
source("R/load_packages.R")
source("R/utils_helpers.R")

sf::sf_use_s2(FALSE)

# --- Inputs
rs <- terra::rast("outputs/count_per_pixel_birdlife_plus_sdms.tif")

front_poly_fsle <- readRDS(
  "outputs/fsle_front_polygons/fsle_quartiles_1994_2022_Q3_pct_cut50.rds"
)

front_poly_thermal <- readRDS(
  "outputs/thermal_front_polygons/thermal_front_persistence_q75_data_median39.rds"
)

land <- get_world_latlon()

# --- Mask land (ocean only)
land_v <- terra::vect(land)
rs <- terra::mask(rs, land_v, inverse = TRUE)

# --- Rasterize polygons to raster grid
r_fsle <- terra::rasterize(
  terra::vect(front_poly_fsle),
  rs,
  field = 1,
  background = NA
)

r_thermal <- terra::rasterize(
  terra::vect(front_poly_thermal),
  rs,
  field = 1,
  background = NA
)

# --- Inside / outside masks
inside_any <- (!is.na(r_fsle)) | (!is.na(r_thermal))
inside_any <- terra::ifel(inside_any, 1, NA)

outside_any <- terra::ifel(is.na(inside_any), 1, NA)

# --- Species presence mask
has_data <- terra::ifel(rs >= 1, 1, NA)

# --- Cell area (km2)
cell_area <- terra::cellSize(rs, unit = "km")

# --- Richness-weighted surface
rich_weight_surface <- cell_area * rs

# --- Latitude raster
lat_r <- terra::init(rs, "y")

# ------------------------------------------------------------------------------
# Ocean domain mask, using same logic as original code
# ------------------------------------------------------------------------------
ocean_mask <- terra::ifel(!is.na(cell_area), 1, NA)

# ------------------------------------------------------------------------------
# Latitude-band masks, restricted to ocean domain
# ------------------------------------------------------------------------------
band_masks <- list(
  global = ocean_mask,
  
  tropics = terra::ifel(
    ocean_mask == 1 & lat_r >= -23.5 & lat_r <= 23.5, 1, NA
  ),
  tropics_north = terra::ifel(
    ocean_mask == 1 & lat_r > 0 & lat_r <= 23.5, 1, NA
  ),
  tropics_south = terra::ifel(
    ocean_mask == 1 & lat_r >= -23.5 & lat_r < 0, 1, NA
  ),
  
  temperate = terra::ifel(
    ocean_mask == 1 & abs(lat_r) > 23.5 & abs(lat_r) <= 60, 1, NA
  ),
  temperate_north = terra::ifel(
    ocean_mask == 1 & lat_r > 23.5 & lat_r <= 60, 1, NA
  ),
  temperate_south = terra::ifel(
    ocean_mask == 1 & lat_r >= -60 & lat_r < -23.5, 1, NA
  ),
  
  polar = terra::ifel(
    ocean_mask == 1 & abs(lat_r) > 60, 1, NA
  ),
  polar_north = terra::ifel(
    ocean_mask == 1 & lat_r > 60, 1, NA
  ),
  polar_south = terra::ifel(
    ocean_mask == 1 & lat_r < -60, 1, NA
  )
)

# ------------------------------------------------------------------------------
# Helper to sum raster values after masking
# ------------------------------------------------------------------------------
sum_masked <- function(x, mask_r) {
  out <- terra::global(
    terra::mask(x, mask_r),
    "sum",
    na.rm = TRUE
  )[1, 1]
  
  if (is.na(out)) out <- 0
  return(out)
}

# ------------------------------------------------------------------------------
# Per-band statistics
# ------------------------------------------------------------------------------
calc_band_stats <- function(band_name, band_mask) {
  
  inside_band <- terra::mask(inside_any, band_mask)
  outside_band <- terra::mask(outside_any, band_mask)
  has_data_band <- terra::mask(has_data, band_mask)
  
  # --- Unweighted
  total_inside_area <- sum_masked(cell_area, inside_band)
  
  inside_area_with_data <- sum_masked(
    cell_area,
    inside_band * has_data_band
  )
  
  total_outside_area <- sum_masked(cell_area, outside_band)
  
  outside_area_with_data <- sum_masked(
    cell_area,
    outside_band * has_data_band
  )
  
  prop_inside <- if (total_inside_area > 0) {
    inside_area_with_data / total_inside_area
  } else {
    NA_real_
  }
  
  prop_outside <- if (total_outside_area > 0) {
    outside_area_with_data / total_outside_area
  } else {
    NA_real_
  }
  
  # --- Richness-weighted
  rich_inside <- sum_masked(rich_weight_surface, inside_band)
  rich_outside <- sum_masked(rich_weight_surface, outside_band)
  
  prop_inside_weighted <- if (total_inside_area > 0) {
    rich_inside / total_inside_area
  } else {
    NA_real_
  }
  
  prop_outside_weighted <- if (total_outside_area > 0) {
    rich_outside / total_outside_area
  } else {
    NA_real_
  }
  
  data.frame(
    band = band_name,
    
    total_inside_area_km2 = total_inside_area,
    inside_area_with_data_km2 = inside_area_with_data,
    prop_inside_with_data = prop_inside,
    
    total_outside_area_km2 = total_outside_area,
    outside_area_with_data_km2 = outside_area_with_data,
    prop_outside_with_data = prop_outside,
    
    inside_richness_weighted_area = rich_inside,
    prop_inside_richness_weighted = prop_inside_weighted,
    
    outside_richness_weighted_area = rich_outside,
    prop_outside_richness_weighted = prop_outside_weighted,
    
    stringsAsFactors = FALSE
  )
}

# ------------------------------------------------------------------------------
# Run all latitude bands
# ------------------------------------------------------------------------------
res <- do.call(rbind, lapply(
  names(band_masks),
  function(nm) calc_band_stats(nm, band_masks[[nm]])
))

# ------------------------------------------------------------------------------
# Output
# ------------------------------------------------------------------------------
print(res)

# Optional nice print
cat("\n--- SUMMARY ---\n")
for (i in seq_len(nrow(res))) {
  cat("\nBand:", res$band[i], "\n")
  
  cat("  Total inside area (km2): ",
      round(res$total_inside_area_km2[i], 2), "\n", sep = "")
  cat("  Inside area with data (km2): ",
      round(res$inside_area_with_data_km2[i], 2), "\n", sep = "")
  cat("  Proportion inside with data: ",
      round(res$prop_inside_with_data[i], 3), "\n", sep = "")
  
  cat("  Total outside area (km2): ",
      round(res$total_outside_area_km2[i], 2), "\n", sep = "")
  cat("  Outside area with data (km2): ",
      round(res$outside_area_with_data_km2[i], 2), "\n", sep = "")
  cat("  Proportion outside with data: ",
      round(res$prop_outside_with_data[i], 3), "\n", sep = "")
  
  cat("  Inside richness-weighted area: ",
      round(res$inside_richness_weighted_area[i], 2), "\n", sep = "")
  cat("  Proportion inside richness-weighted: ",
      round(res$prop_inside_richness_weighted[i], 3), "\n", sep = "")
  
  cat("  Outside richness-weighted area: ",
      round(res$outside_richness_weighted_area[i], 2), "\n", sep = "")
  cat("  Proportion outside richness-weighted: ",
      round(res$prop_outside_richness_weighted[i], 3), "\n", sep = "")
}

# Optional write
# write.csv(res, "outputs/area_stats_inside_outside_by_lat_band.csv", row.names = FALSE)