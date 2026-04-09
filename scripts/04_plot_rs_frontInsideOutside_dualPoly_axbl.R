# ==============================================================================
#  This code was created by Isaac Brito-Morales
#  (ibrito@conservation.org)
# ==============================================================================

# ==============================================================================
# Species-at-threat map inside and outside front hotspot polygons
#
# Purpose:
#   - Read the species-at-threat raster
#   - Read FSLE and thermal front hotspot polygons
#   - Classify species pixels as inside or outside the union of both polygon sets
#   - Plot species at threat using:
#       * full color scale inside hotspot polygons
#       * grey scale outside hotspot polygons
#   - Overlay both polygon outlines in the final map
#
# Expected input:
#   outputs/count_per_pixel_birdlife_plus_sdms.tif
#   outputs/fsle_front_polygons/fsle_quartiles_1994_2022_Q3_pct_cut50.rds
#   outputs/thermal_front_polygons/thermal_front_persistence_q75_data_median39.rds
#
# Creates:
#   /home/SB5/species_threat_fronts_AquaXBirdlife_insideOutside_dualPoly_v01a.png
#
# Notes:
# - Species at threat is the only raster variable plotted
# - No front magnitude raster is used in this version
# - Inside pixels are defined relative to the union of FSLE and thermal polygons
# - Outside pixels are shown with a grey scale based on species-at-threat values
# - Polygon geometries are cleaned and wrapped across the dateline before plotting
# - Robinson projection and map styling follow previous figure conventions
# ==============================================================================

# --- Project runtime (safe to call repeatedly)
source("renv/activate.R")

source("R/load_packages.R")
source("R/utils_helpers.R")

# Disable s2 spherical geometry (avoids invalid-loop errors for raster-derived polygons)
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

# --- User options
species_palette <- "YlOrRd"
species_direction <- 1

fsle_poly_color <- "firebrick3"
thermal_poly_color <- "dodgerblue3"
front_poly_linewidth <- 0.3

# Optional polygon simplification for cleaner publication outlines
simplify_front_polygons <- TRUE
simplify_tolerance <- 0.05
simplify_preserve_topology <- TRUE

# --- Output
out_file <- if (isTRUE(simplify_front_polygons)) {
  "/home/SB5/species_threat_fronts_AquaXBirdlife_insideOutside_dualPoly_v01a_simplified.png"
} else {
  "/home/SB5/species_threat_fronts_AquaXBirdlife_insideOutside_dualPoly_v01a.png"
}

# outside grey scale
outside_low <- "grey92"
outside_high <- "grey35"

# --- Ensure output dir exists
dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)

# --- Mask land (ocean only)
land_v <- terra::vect(land)
rs <- terra::mask(rs, land_v, inverse = TRUE)

# --- Species raster to df
df <- as.data.frame(rs, xy = TRUE, na.rm = FALSE)
names(df) <- c("x", "y", "val")
df <- df[!is.na(df$val) & df$val > 0, ]

# --- Auto limits and breaks for inside scale
species_min <- 1
species_max <- max(df$val, na.rm = TRUE)

brks_inside <- pretty(c(species_min, species_max), n = 8)
brks_inside <- brks_inside[brks_inside >= species_min & brks_inside <= species_max]

# --- Polygon cleaning helper
clean_wrap_poly <- function(x) {
  x <- sf::st_as_sf(x)
  x <- sf::st_make_valid(x)

  x <- sf::st_wrap_dateline(
    x,
    options = c("WRAPDATELINE=YES", "DATELINEOFFSET=180"),
    quiet = TRUE
  )

  sf::st_make_valid(x)
}

simplify_front_poly <- function(x,
                                do_simplify = FALSE,
                                d_tolerance = 0.05,
                                preserve_topology = TRUE) {
  if (!isTRUE(do_simplify)) {
    return(x)
  }

  x <- sf::st_simplify(
    x,
    dTolerance = d_tolerance,
    preserveTopology = preserve_topology
  )

  sf::st_make_valid(x)
}

# --- Clean and wrap polygons separately for plotting
front_poly_fsle_plot <- clean_wrap_poly(front_poly_fsle)
front_poly_thermal_plot <- clean_wrap_poly(front_poly_thermal)

front_poly_fsle_plot <- simplify_front_poly(
  front_poly_fsle_plot,
  do_simplify = simplify_front_polygons,
  d_tolerance = simplify_tolerance,
  preserve_topology = simplify_preserve_topology
)

front_poly_thermal_plot <- simplify_front_poly(
  front_poly_thermal_plot,
  do_simplify = simplify_front_polygons,
  d_tolerance = simplify_tolerance,
  preserve_topology = simplify_preserve_topology
)

# --- Convert species pixels to sf points and classify inside/outside
pts <- sf::st_as_sf(df, coords = c("x", "y"), crs = 4326, remove = FALSE)

inside_fsle <- lengths(sf::st_intersects(pts, front_poly_fsle_plot)) > 0
inside_thermal <- lengths(sf::st_intersects(pts, front_poly_thermal_plot)) > 0
inside_idx <- inside_fsle | inside_thermal

df$zone <- ifelse(inside_idx, "inside", "outside")

df_inside <- df[df$zone == "inside", , drop = FALSE]
df_outside <- df[df$zone == "outside", , drop = FALSE]

# --- Breaks for outside scale using outside-only values
outside_min <- 1
if (nrow(df_outside) > 0) {
  outside_max <- max(df_outside$val, na.rm = TRUE)
  brks_outside <- pretty(c(outside_min, outside_max), n = 6)
  brks_outside <- brks_outside[brks_outside >= outside_min & brks_outside <= outside_max]
} else {
  outside_max <- species_max
  brks_outside <- brks_inside
}

# --- Earth outline
lon <- seq(-180, 180, by = 0.5)
lat <- seq(-90, 90, by = 0.5)

ring <- rbind(
  cbind(lon,  90),
  cbind(rep( 180, length(lat)), rev(lat)),
  cbind(rev(lon), -90),
  cbind(rep(-180, length(lat)), lat),
  cbind(lon,  90)
)

earth_outline <- sf::st_sfc(
  sf::st_linestring(ring),
  crs = 4326
) |>
  sf::st_transform(robin)

# --- Theme
theme_map <- ggplot2::theme_void() +
  ggplot2::theme(
    panel.grid.major = ggplot2::element_line(color = "grey80", linewidth = 0.3),
    panel.grid.minor = ggplot2::element_blank(),
    axis.text  = ggplot2::element_text(color = "grey30", size = 9),
    axis.title = ggplot2::element_blank(),
    legend.title = ggplot2::element_text(size = 10),
    legend.text  = ggplot2::element_text(size = 9),
    legend.box = "vertical"
  )

# --- Plot
p4 <- ggplot2::ggplot() +

  # inside first, full color scale
  ggplot2::geom_tile(
    data = df_inside,
    ggplot2::aes(x = x, y = y, fill = val),
    na.rm = TRUE
  ) +

  ggplot2::scale_fill_distiller(
    name      = "Species at threat\n(inside)",
    palette   = species_palette,
    breaks    = brks_inside,
    labels    = scales::label_number(accuracy = 1),
    limits    = c(species_min, species_max),
    oob       = scales::squish,
    na.value  = NA,
    direction = species_direction,
    guide     = ggplot2::guide_colourbar(order = 1)
  ) +

  ggnewscale::new_scale_fill() +

  # outside second, grey scale
  ggplot2::geom_tile(
    data = df_outside,
    ggplot2::aes(x = x, y = y, fill = val),
    na.rm = TRUE
  ) +

  ggplot2::scale_fill_gradient(
    name = "Species at threat\n(outside)",
    low = outside_low,
    high = outside_high,
    breaks = brks_outside,
    labels = scales::label_number(accuracy = 1),
    limits = c(outside_min, outside_max),
    oob = scales::squish,
    na.value = NA,
    guide = ggplot2::guide_colourbar(order = 2)
  ) +

  # FSLE outline
  ggplot2::geom_sf(
    data = front_poly_fsle_plot,
    fill = NA,
    color = fsle_poly_color,
    linewidth = front_poly_linewidth,
    inherit.aes = FALSE
  ) +

  # Thermal outline
  ggplot2::geom_sf(
    data = front_poly_thermal_plot,
    fill = NA,
    color = thermal_poly_color,
    linewidth = front_poly_linewidth,
    inherit.aes = FALSE
  ) +

  # Land
  ggplot2::geom_sf(
    data = land,
    fill = "grey20",
    color = "grey30",
    linewidth = 0.2,
    inherit.aes = FALSE
  ) +

  # Earth outline
  ggplot2::geom_sf(
    data = earth_outline,
    color = "grey50",
    linewidth = 1.0,
    inherit.aes = FALSE
  ) +

  ggplot2::coord_sf(
    crs = robin,
    default_crs = sf::st_crs(4326),
    expand = FALSE
  ) +

  theme_map

ggplot2::ggsave(
  filename = out_file,
  plot = p4,
  width = 14,
  height = 7,
  units = "in",
  dpi = 300,
  bg = "white",
  device = ragg::agg_png
)

message("Saved figure to: ", out_file)
