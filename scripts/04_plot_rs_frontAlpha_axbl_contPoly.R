# =============================================================================
# Species-at-threat raster + variable-opacity front logic + front polygons
#
# Author: Isaac Brito-Morales
# Email: ibrito@conservation.org
#
# Output:
#   outputs/figures/exploratory/species_threat_fronts_AquaXBirdlife_frontPolyContAlpha_v01a.png
# =============================================================================

# --- Project runtime (safe to call repeatedly)
source("renv/activate.R")

source("R/load_packages.R")
source("R/utils_helpers.R")

# Disable s2 spherical geometry (avoids invalid-loop errors for raster-derived polygons)
sf::sf_use_s2(FALSE)

# --- Inputs
rs <- terra::rast("outputs/count_per_pixel_birdlife_plus_sdms.tif")
front_pct <- terra::rast("outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q3_pct.tif")
front_poly <- readRDS("outputs/fsle_front_polygons/fsle_quartiles_1994_2022_Q3_pct_cut50.rds")
land <- get_world_latlon()

# --- User options
species_palette <- "YlOrRd"
species_direction <- 1

thr <- 50

# Alpha logic:
# - front >= thr  -> full opacity
# - front <  thr  -> progressively fainter
#   * alpha at front = 0   -> alpha_min_below
#   * alpha at front ~ 50  -> alpha_max_below
alpha_min_below <- 0.12
alpha_max_below <- 0.75

front_poly_color <- "firebrick3"
front_poly_linewidth <- 0.3

# --- Mask land (ocean only)
land_v <- terra::vect(land)
rs <- terra::mask(rs, land_v, inverse = TRUE)

# --- Species raster to df
df <- as.data.frame(rs, xy = TRUE, na.rm = FALSE)
names(df) <- c("x", "y", "val")
df <- df[!is.na(df$val) & df$val > 0, ]

# --- Auto limits and breaks
species_min <- 1
species_max <- max(df$val, na.rm = TRUE)

brks <- pretty(c(species_min, species_max), n = 8)
brks <- brks[brks >= species_min & brks <= species_max]

# --- Front raster preparation
front_pct <- terra::rotate(front_pct)
front_pct_rs <- terra::resample(front_pct, rs, method = "bilinear")
front_pct_rs <- terra::mask(front_pct_rs, land_v, inverse = TRUE)

# --- Join front values to species df
front_df <- as.data.frame(front_pct_rs, xy = TRUE, na.rm = FALSE)
names(front_df) <- c("x", "y", "front")

df <- merge(df, front_df, by = c("x", "y"), all.x = TRUE, sort = FALSE)

# --- Build variable alpha directly on species layer
df$alpha_species <- NA_real_

# full opacity for >= threshold
ok_full <- !is.na(df$front) & df$front >= thr
df$alpha_species[ok_full] <- 1

# progressively fainter for < threshold
ok_low <- !is.na(df$front) & df$front < thr
df$alpha_species[ok_low] <- alpha_min_below +
  (alpha_max_below - alpha_min_below) * (df$front[ok_low] / thr)

# if front is NA, keep faint but visible
df$alpha_species[is.na(df$front)] <- alpha_min_below

# --- Front polygons cleaning + dateline wrapping
front_poly <- sf::st_as_sf(front_poly)
front_poly_mask <- sf::st_make_valid(front_poly)
front_poly_mask <- sf::st_union(front_poly_mask)

front_poly_plot <- sf::st_wrap_dateline(
  front_poly_mask,
  options = c("WRAPDATELINE=YES", "DATELINEOFFSET=180"),
  quiet = TRUE
)

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
    legend.text  = ggplot2::element_text(size = 9)
  )

# --- Plot
p4 <- ggplot2::ggplot() +

  ggplot2::geom_tile(
    data = df,
    ggplot2::aes(x = x, y = y, fill = val, alpha = alpha_species),
    na.rm = TRUE
  ) +

  ggplot2::scale_fill_distiller(
    name      = "Species at threat (n)",
    palette   = species_palette,
    breaks    = brks,
    labels    = scales::label_number(accuracy = 1),
    limits    = c(species_min, species_max),
    oob       = scales::squish,
    na.value  = NA,
    direction = species_direction
  ) +

  ggplot2::scale_alpha_identity(guide = "none") +

  ggplot2::geom_sf(
    data = front_poly_plot,
    fill = NA,
    color = front_poly_color,
    linewidth = front_poly_linewidth,
    inherit.aes = FALSE
  ) +

  ggplot2::geom_sf(
    data = land,
    fill = "grey20",
    color = "grey30",
    linewidth = 0.2,
    inherit.aes = FALSE
  ) +

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
  filename = "outputs/figures/exploratory/species_threat_fronts_AquaXBirdlife_frontPolyContAlpha_v01a.png",
  plot = p4,
  width = 14,
  height = 7,
  units = "in",
  dpi = 300,
  bg = "white",
  device = ragg::agg_png
)