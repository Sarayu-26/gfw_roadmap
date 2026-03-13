# =============================================================================
# Summarize species-at-threat inside vs outside ocean front polygons
#
# Author: Isaac Brito-Morales
# Email: ibrito@conservation.org
#
# Purpose:
#   - Compute summary tables comparing species-at-threat inside vs outside
#     front polygons.
#   - Uses a single species-at-threat raster and a front-polygon layer that can
#     represent either FSLE or thermal fronts.
#   - Produces outputs by latitude band for:
#       1) presence / absence summaries
#       2) area summaries in km2
#       3) species-area index summaries (species·km2)
#       4) mean species per occupied km2
#
# Key ideas:
#   - Species raster stays constant across workflows.
#   - Front polygons are rasterized back to the species grid to define inside
#     vs outside front regions.
#   - Cell area is computed in km2 because lat/lon grids do not have equal-area
#     cells globally.
#   - Outputs are produced for:
#       all, trop, temp, temp_north, temp_south
# =============================================================================

# --- Project runtime (safe to call repeatedly)
source("renv/activate.R")

source("R/load_packages.R")
source("R/utils_helpers.R")

# Use planar operations for polygon handling
sf::sf_use_s2(FALSE)

# -----------------------------------------------------------------------------
# Helper function
# -----------------------------------------------------------------------------
run_front_summary <- function(
    species_raster,
    front_polygon,
    front_type = c("fsle", "thermal"),
    outdir = "outputs"
) {
  
  front_type <- match.arg(front_type)
  
  if (!dir.exists(outdir)) {
    dir.create(outdir, recursive = TRUE)
  }
  
  # ---------------------------------------------------------------------------
  # 1) Inputs
  # ---------------------------------------------------------------------------
  rs <- terra::rast(species_raster)
  front_poly <- readRDS(front_polygon)
  land <- get_world_latlon()
  
  # ---------------------------------------------------------------------------
  # 2) Mask land so analyses use ocean cells only
  # ---------------------------------------------------------------------------
  land_vect <- terra::vect(land)
  rs <- terra::mask(rs, land_vect, inverse = TRUE)
  
  # ---------------------------------------------------------------------------
  # 3) Build base species table exactly following old 6b logic
  # ---------------------------------------------------------------------------
  df_raw <- as.data.frame(rs, xy = TRUE, na.rm = FALSE)
  colnames(df_raw) <- c("x", "y", "val")
  
  # ---------------------------------------------------------------------------
  # 4) Clean front polygons for masking
  # ---------------------------------------------------------------------------
  front_poly_mask <- sf::st_make_valid(front_poly)
  front_poly_mask <- sf::st_collection_extract(front_poly_mask, "POLYGON")
  front_poly_mask <- sf::st_union(front_poly_mask)
  
  # ---------------------------------------------------------------------------
  # 5) Rasterize polygons to the same grid as species raster
  # ---------------------------------------------------------------------------
  front_vect <- terra::vect(front_poly_mask)
  front_mask <- terra::rasterize(front_vect, rs, field = 1)
  
  # ---------------------------------------------------------------------------
  # 6) Reproduce old 6b alignment exactly
  # ---------------------------------------------------------------------------
  df_pres <- df_raw
  valid <- !is.na(df_pres$val)
  df_pres <- df_pres[valid, , drop = FALSE]
  
  inside_pres <- !is.na(terra::values(front_mask))
  inside_pres <- inside_pres[valid]
  
  # ---------------------------------------------------------------------------
  # 7) Add cell area in km2 using the same valid mask
  # ---------------------------------------------------------------------------
  area_rs <- terra::cellSize(rs, unit = "km")
  area_vals <- terra::values(area_rs, mat = FALSE)
  df_pres$cell_area_km2 <- area_vals[valid]
  
  # Subsets (same backbone as old code)
  df_all_pres <- df_pres
  df_in_pres  <- df_pres[inside_pres, , drop = FALSE]
  df_out_pres <- df_pres[!inside_pres, , drop = FALSE]
  
  # ---------------------------------------------------------------------------
  # 8) Latitude-band helpers (same as old code)
  # ---------------------------------------------------------------------------
  is_trop <- function(d) d$y >= -23.5 & d$y <= 23.5
  is_temp <- function(d) abs(d$y) > 23.5 & abs(d$y) <= 60
  is_temp_north <- function(d) d$y > 23.5 & d$y <= 60
  is_temp_south <- function(d) d$y < -23.5 & d$y >= -60
  
  bands <- c("all", "trop", "temp", "temp_north", "temp_south")
  
  band_filter <- function(d, band) {
    if (band == "all") return(d)
    if (band == "trop") return(d[is_trop(d), , drop = FALSE])
    if (band == "temp") return(d[is_temp(d), , drop = FALSE])
    if (band == "temp_north") return(d[is_temp_north(d), , drop = FALSE])
    if (band == "temp_south") return(d[is_temp_south(d), , drop = FALSE])
    stop("Unknown band: ", band)
  }
  
  # ---------------------------------------------------------------------------
  # 9) Summary helpers
  # ---------------------------------------------------------------------------
  presence_stats <- function(d) {
    if (nrow(d) == 0) {
      return(c(
        total_cells = 0,
        presence_cells = 0,
        presence_pct = NA_real_
      ))
    }
    pres <- d$val > 0
    c(
      total_cells = length(pres),
      presence_cells = sum(pres, na.rm = TRUE),
      presence_pct = mean(pres, na.rm = TRUE)
    )
  }
  
  area_stats <- function(d) {
    if (nrow(d) == 0) {
      return(c(
        total_area_km2 = 0,
        presence_area_km2 = 0
      ))
    }
    pres <- d$val > 0
    c(
      total_area_km2 = sum(d$cell_area_km2, na.rm = TRUE),
      presence_area_km2 = sum(d$cell_area_km2[pres], na.rm = TRUE)
    )
  }
  
  species_area_stats <- function(d) {
    if (nrow(d) == 0) {
      return(c(
        species_area_index = 0,
        mean_species_per_occupied_km2 = NA_real_
      ))
    }
    
    pres <- d$val > 0
    species_area_index <- sum(d$val * d$cell_area_km2, na.rm = TRUE)
    occupied_area <- sum(d$cell_area_km2[pres], na.rm = TRUE)
    
    c(
      species_area_index = species_area_index,
      mean_species_per_occupied_km2 = if (occupied_area > 0) {
        species_area_index / occupied_area
      } else {
        NA_real_
      }
    )
  }
  
  safe_ratio <- function(num, den, digits = 2) {
    if (is.na(num) || is.na(den) || den == 0) return(NA_real_)
    round(num / den, digits)
  }
  
  # ---------------------------------------------------------------------------
  # 10) Presence table (same structure as old 6b output)
  # ---------------------------------------------------------------------------
  presence_tbl <- do.call(rbind, lapply(bands, function(b) {
    d_in  <- band_filter(df_in_pres, b)
    d_out <- band_filter(df_out_pres, b)
    
    p_in <- presence_stats(d_in)
    p_out <- presence_stats(d_out)
    
    data.frame(
      front_type = front_type,
      band = b,
      inside_total_cells = unname(p_in["total_cells"]),
      inside_presence_cells = unname(p_in["presence_cells"]),
      inside_presence_pct = round(unname(p_in["presence_pct"]), 3),
      outside_total_cells = unname(p_out["total_cells"]),
      outside_presence_cells = unname(p_out["presence_cells"]),
      outside_presence_pct = round(unname(p_out["presence_pct"]), 3),
      ratio_to_outside = safe_ratio(
        unname(p_in["presence_pct"]),
        unname(p_out["presence_pct"])
      ),
      units_inside_total_cells = "cells",
      units_inside_presence_cells = "cells",
      units_inside_presence_pct = "proportion",
      units_outside_total_cells = "cells",
      units_outside_presence_cells = "cells",
      units_outside_presence_pct = "proportion",
      units_ratio_to_outside = "unitless",
      row.names = NULL,
      check.names = FALSE
    )
  }))
  
  # ---------------------------------------------------------------------------
  # 11) Area table
  # ---------------------------------------------------------------------------
  area_tbl <- do.call(rbind, lapply(bands, function(b) {
    d_in  <- band_filter(df_in_pres, b)
    d_out <- band_filter(df_out_pres, b)
    
    a_in <- area_stats(d_in)
    a_out <- area_stats(d_out)
    
    data.frame(
      front_type = front_type,
      band = b,
      inside_total_area_km2 = round(unname(a_in["total_area_km2"]), 2),
      inside_presence_area_km2 = round(unname(a_in["presence_area_km2"]), 2),
      outside_total_area_km2 = round(unname(a_out["total_area_km2"]), 2),
      outside_presence_area_km2 = round(unname(a_out["presence_area_km2"]), 2),
      ratio_presence_area_to_outside = safe_ratio(
        unname(a_in["presence_area_km2"]),
        unname(a_out["presence_area_km2"])
      ),
      units_inside_total_area_km2 = "km2",
      units_inside_presence_area_km2 = "km2",
      units_outside_total_area_km2 = "km2",
      units_outside_presence_area_km2 = "km2",
      units_ratio_presence_area_to_outside = "unitless",
      row.names = NULL,
      check.names = FALSE
    )
  }))
  
  # ---------------------------------------------------------------------------
  # 12) Species-area index table
  # ---------------------------------------------------------------------------
  species_area_tbl <- do.call(rbind, lapply(bands, function(b) {
    d_in  <- band_filter(df_in_pres, b)
    d_out <- band_filter(df_out_pres, b)
    
    s_in <- species_area_stats(d_in)
    s_out <- species_area_stats(d_out)
    
    data.frame(
      front_type = front_type,
      band = b,
      inside_species_area_index = round(unname(s_in["species_area_index"]), 2),
      outside_species_area_index = round(unname(s_out["species_area_index"]), 2),
      ratio_species_area_to_outside = safe_ratio(
        unname(s_in["species_area_index"]),
        unname(s_out["species_area_index"])
      ),
      units_inside_species_area_index = "species·km2",
      units_outside_species_area_index = "species·km2",
      units_ratio_species_area_to_outside = "unitless",
      row.names = NULL,
      check.names = FALSE
    )
  }))
  
  # ---------------------------------------------------------------------------
  # 13) Species density table
  # ---------------------------------------------------------------------------
  density_tbl <- do.call(rbind, lapply(bands, function(b) {
    d_in  <- band_filter(df_in_pres, b)
    d_out <- band_filter(df_out_pres, b)
    
    s_in <- species_area_stats(d_in)
    s_out <- species_area_stats(d_out)
    
    data.frame(
      front_type = front_type,
      band = b,
      inside_mean_species_per_occupied_km2 = round(
        unname(s_in["mean_species_per_occupied_km2"]), 6
      ),
      outside_mean_species_per_occupied_km2 = round(
        unname(s_out["mean_species_per_occupied_km2"]), 6
      ),
      ratio_density_to_outside = safe_ratio(
        unname(s_in["mean_species_per_occupied_km2"]),
        unname(s_out["mean_species_per_occupied_km2"])
      ),
      units_inside_mean_species_per_occupied_km2 = "species/km2",
      units_outside_mean_species_per_occupied_km2 = "species/km2",
      units_ratio_density_to_outside = "unitless",
      row.names = NULL,
      check.names = FALSE
    )
  }))
  
  # ---------------------------------------------------------------------------
  # 14) Write outputs
  # ---------------------------------------------------------------------------
  write.csv(
    presence_tbl,
    file.path(outdir, paste0("summary_presence_inside_vs_outside_by_latband_", front_type, ".csv")),
    row.names = FALSE
  )
  
  write.csv(
    area_tbl,
    file.path(outdir, paste0("summary_area_inside_vs_outside_by_latband_", front_type, ".csv")),
    row.names = FALSE
  )
  
  write.csv(
    species_area_tbl,
    file.path(outdir, paste0("summary_species_area_index_by_latband_", front_type, ".csv")),
    row.names = FALSE
  )
  
  write.csv(
    density_tbl,
    file.path(outdir, paste0("summary_species_density_by_latband_", front_type, ".csv")),
    row.names = FALSE
  )
  
  invisible(list(
    presence = presence_tbl,
    area = area_tbl,
    species_area = species_area_tbl,
    density = density_tbl
  ))
}

# -----------------------------------------------------------------------------
# Runs
# -----------------------------------------------------------------------------

# FSLE
run_front_summary(
  species_raster = "outputs/count_per_pixel_birdlife_plus_sdms.tif",
  front_polygon = "outputs/fsle_front_polygons/fsle_quartiles_1994_2022_Q3_pct_cut50.rds",
  front_type = "fsle",
  outdir = "outputs"
)

# Thermal
run_front_summary(
  species_raster = "outputs/count_per_pixel_birdlife_plus_sdms.tif",
  front_polygon = "outputs/thermal_front_polygons/thermal_front_persistence_q90_minpatch20.rds",
  front_type = "thermal",
  outdir = "outputs"
)
