# ==============================================================================
#  Area statistics: inside vs outside front polygons
#
#  Purpose:
#   - Compute total area inside (FSLE OR thermal)
#   - Compute area with data (val >= 1) inside
#   - Compute total area outside
#   - Compute area with data outside
#   - Derive proportions
#
#  Notes:
#   - Uses raster grid (no polygon union)
#   - "inside" = FSLE OR thermal OR both
#   - "with data" = rs >= 1
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

# --- Rasterize polygons to raster grid (1 = inside, NA = outside)

# FSLE
r_fsle <- terra::rasterize(
  terra::vect(front_poly_fsle),
  rs,
  field = 1,
  background = NA
)

# Thermal
r_thermal <- terra::rasterize(
  terra::vect(front_poly_thermal),
  rs,
  field = 1,
  background = NA
)

# --- Inside = FSLE OR thermal (union without merging polygons)
inside_any <- (!is.na(r_fsle)) | (!is.na(r_thermal))

# Convert logical to numeric mask (1 / NA)
inside_any <- terra::ifel(inside_any, 1, NA)

# --- Outside mask
outside_any <- terra::ifel(is.na(inside_any), 1, NA)

# --- Data mask (>= 1)
has_data <- terra::ifel(rs >= 1, 1, NA)

# --- Cell area (km²)
cell_area <- terra::cellSize(rs, unit = "km")

# --- Area calculations

# Inside total
total_inside_area <- terra::global(
  terra::mask(cell_area, inside_any),
  "sum",
  na.rm = TRUE
)[1,1]

# Inside with data
inside_area_with_data <- terra::global(
  terra::mask(cell_area, inside_any * has_data),
  "sum",
  na.rm = TRUE
)[1,1]

# Outside total
total_outside_area <- terra::global(
  terra::mask(cell_area, outside_any),
  "sum",
  na.rm = TRUE
)[1,1]

# Outside with data
outside_area_with_data <- terra::global(
  terra::mask(cell_area, outside_any * has_data),
  "sum",
  na.rm = TRUE
)[1,1]

# --- Proportions
prop_inside  <- inside_area_with_data  / total_inside_area
prop_outside <- outside_area_with_data / total_outside_area

# --- Output
res <- data.frame(
  metric = c(
    "total_inside_area_km2",
    "inside_area_with_data_km2",
    "total_outside_area_km2",
    "outside_area_with_data_km2",
    "prop_inside_with_data",
    "prop_outside_with_data"
  ),
  value = c(
    total_inside_area,
    inside_area_with_data,
    total_outside_area,
    outside_area_with_data,
    prop_inside,
    prop_outside
  )
)

print(res)

# Optional nice print
cat("\n--- SUMMARY ---\n")
cat("Total inside area (km2): ", round(total_inside_area, 2), "\n")
cat("Inside area with data (km2): ", round(inside_area_with_data, 2), "\n")
cat("Proportion inside with data: ", round(prop_inside, 3), "\n\n")

cat("Total outside area (km2): ", round(total_outside_area, 2), "\n")
cat("Outside area with data (km2): ", round(outside_area_with_data, 2), "\n")
cat("Proportion outside with data: ", round(prop_outside, 3), "\n")