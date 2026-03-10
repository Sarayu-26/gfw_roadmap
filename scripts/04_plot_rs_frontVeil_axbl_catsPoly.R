# =============================================================================
# Species-at-threat raster + FRONT raster veil + front polygon delineation
#
# Author: Isaac Brito-Morales
# Email: ibrito@conservation.org
#
# Output:
#   outputs/figures/exploratory/species_threat_fronts_AquaXBirdlife_frontPolyCats_v01a.png
# =============================================================================

# --- Project runtime (safe to call repeatedly)
if (requireNamespace("renv", quietly = TRUE)) {
  try(renv::activate(), silent = TRUE)
}

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
species_breaks <- c(0, 20, 35, 50, Inf)
species_labels <- c("1-20", "20-35", "35-50", "50+")
species_palette <- "YlOrRd"

thr <- 50
max_veil_alpha <- 0.45

front_poly_color <- "firebrick3"
front_poly_linewidth <- 0.3

# --- Mask land (ocean only)
land_v <- terra::vect(land)
rs <- terra::mask(rs, land_v, inverse = TRUE)

# --- Species raster to df
df <- as.data.frame(rs, xy = TRUE, na.rm = FALSE)
names(df) <- c("x", "y", "val")
df <- df[!is.na(df$val) & df$val > 0, ]

# --- Categorize species-at-threat
df$val_cat <- cut(
  df$val,
  breaks = species_breaks,
  include.lowest = FALSE,
  right = TRUE,
  labels = species_labels
)

df <- df[!is.na(df$val_cat), ]
df$val_cat <- factor(df$val_cat, levels = species_labels, ordered = TRUE)

# --- Front raster preparation
front_pct <- terra::rotate(front_pct)
front_pct_rs <- terra::resample(front_pct, rs, method = "bilinear")
front_pct_rs <- terra::mask(front_pct_rs, land_v, inverse = TRUE)

# --- Veil alpha
front_df <- as.data.frame(front_pct_rs, xy = TRUE, na.rm = FALSE)
names(front_df) <- c("x", "y", "front")

front_df$alpha_norm <- NA_real_
ok <- !is.na(front_df$front) & front_df$front < thr
front_df$alpha_norm[ok] <- 1 - (front_df$front[ok] / thr)

veil_df <- front_df[!is.na(front_df$alpha_norm), c("x", "y", "alpha_norm")]

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

# --- Palette
species_cols <- RColorBrewer::brewer.pal(length(species_labels), species_palette)

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
  
  geom_tile(
    data = df,
    ggplot2::aes(x = x, y = y, fill = val_cat),
    na.rm = TRUE
  ) +
  
  ggplot2::scale_fill_manual(
    name = "Species at threat (n)",
    values = stats::setNames(species_cols, species_labels),
    drop = FALSE,
    na.value = NA,
    guide = ggplot2::guide_legend(reverse = TRUE)
  ) +
  
  geom_tile(
    data = veil_df,
    ggplot2::aes(x = x, y = y, alpha = alpha_norm),
    fill = "grey70",
    inherit.aes = FALSE
  ) +
  
  ggplot2::scale_alpha(range = c(0, max_veil_alpha), guide = "none") +
  
  geom_sf(
    data = front_poly_plot,
    fill = NA,
    color = front_poly_color,
    linewidth = front_poly_linewidth,
    inherit.aes = FALSE
  ) +
  
  geom_sf(
    data = land,
    fill = "grey20",
    color = "grey30",
    linewidth = 0.2,
    inherit.aes = FALSE
  ) +
  
  geom_sf(
    data = earth_outline,
    color = "grey50",
    linewidth = 1.0,
    inherit.aes = FALSE
  ) +
  
  coord_sf(
    crs = robin,
    default_crs = sf::st_crs(4326),
    expand = FALSE
  ) +
  
  theme_map

ggplot2::ggsave(
  filename = "outputs/figures/exploratory/species_threat_fronts_AquaXBirdlife_frontPolyCats_v01a.png",
  plot = p4,
  width = 14,
  height = 7,
  units = "in",
  dpi = 300,
  bg = "white",
  device = ragg::agg_png
)