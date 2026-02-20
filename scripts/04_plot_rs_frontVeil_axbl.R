# =============================================================================
# Species-at-threat raster + FRONT raster veil (opacity varies for <50%)
#
# Author: Isaac Brito-Morales
# Email: ibrito@conservation.org
#
# Output:
#   outputs/figures/exploratory/species_threat_fronts_AquaXBirdlife_v01c-4.png
#
# Notes:
#   - Uses renv (if present) + centralized loader for consistent packages.
#   - Uses utils_helpers for projections + world polygons.
#   - Avoids redefining CRS strings already defined in utils_helpers.R
# =============================================================================

# --- Project runtime (safe to call repeatedly)
if (requireNamespace("renv", quietly = TRUE)) {
  try(renv::activate(), silent = TRUE)
}

source("R/load_packages.R")
source("R/utils_helpers.R")

# --- Inputs
rs <- terra::rast("outputs/count_per_pixel_birdlife_plus_sdms.tif")  # Species at threat (n)
front_pct <- terra::rast("outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q3_pct.tif")  # 0–100
land <- get_world_latlon()  # WGS84 land polygons (sf)

# --- Mask land (ocean only)
land_v <- terra::vect(land)
rs <- terra::mask(rs, land_v, inverse = TRUE)

# --- Species raster to df (keep only >0)
df <- as.data.frame(rs, xy = TRUE, na.rm = FALSE)
names(df) <- c("x", "y", "val")
df <- df[!is.na(df$val) & df$val > 0, ]

# --- Front raster: rotate 0..360 -> -180..180, match grid, mask land
front_pct <- terra::rotate(front_pct)
front_pct_rs <- terra::resample(front_pct, rs, method = "bilinear")
front_pct_rs <- terra::mask(front_pct_rs, land_v, inverse = TRUE)

# --- Build variable veil alpha for pixels < 50%
thr <- 50
max_veil_alpha <- 0.45  # max opacity at frontiness = 0; fades to 0 as it approaches 50

front_df <- as.data.frame(front_pct_rs, xy = TRUE, na.rm = FALSE)
names(front_df) <- c("x", "y", "front")

# alpha_norm in [0,1] for front < thr; NA for >= thr or NA (so no veil there)
front_df$alpha_norm <- NA_real_
ok <- !is.na(front_df$front) & front_df$front < thr
front_df$alpha_norm[ok] <- 1 - (front_df$front[ok] / thr)

veil_df <- front_df[!is.na(front_df$alpha_norm), c("x", "y", "alpha_norm")]

# --- Earth outline (true projection boundary)
lon <- seq(-180, 180, by = 0.5)
lat <- seq(-90, 90, by = 0.5)
ring <- rbind(
  cbind(lon,  90),
  cbind(rep( 180, length(lat)), rev(lat)),
  cbind(rev(lon), -90),
  cbind(rep(-180, length(lat)), lat),
  cbind(lon,  90)
)
earth_outline <- sf::st_sfc(sf::st_linestring(ring), crs = 4326) |>
  sf::st_transform(robin)  # robin comes from utils_helpers.R

# --- Breaks
brks <- c(1, 5, 10, 20, 30, 40, 50, 60, 70, 80)

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
  # Species raster
  ggplot2::geom_tile(
    data = df,
    ggplot2::aes(x = x, y = y, fill = val),
    na.rm = TRUE
  ) +
  ggplot2::scale_fill_distiller(
    name      = "Species at threat (n)",
    palette   = "RdYlBu",
    breaks    = brks,
    labels    = scales::label_number(accuracy = 1),
    limits    = c(1, 74),
    oob       = scales::squish,
    na.value  = NA,
    direction = -1
  ) +
  # Variable veil for <50% (more opaque when less fronty)
  ggplot2::geom_tile(
    data = veil_df,
    ggplot2::aes(x = x, y = y, alpha = alpha_norm),
    fill = "grey70",
    inherit.aes = FALSE
  ) +
  ggplot2::scale_alpha(range = c(0, max_veil_alpha), guide = "none") +
  # Land + outline
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
  ggplot2::coord_sf(crs = robin, default_crs = sf::st_crs(4326), expand = FALSE) +
  theme_map

ggplot2::ggsave(
  filename = "outputs/figures/exploratory/species_threat_fronts_AquaXBirdlife_v01c-4.png",
  plot = p4,
  width = 14, height = 7, units = "in",
  dpi = 300, bg = "white",
  device = ragg::agg_png
)