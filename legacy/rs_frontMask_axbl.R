# =============================================================================
# Species-at-threat raster + FRONT raster mask overlay (shade out non-front)
# Output: outputs/figures/exploratory/species_threat_fronts_AquaXBirdlife_v01c-2.png
# =============================================================================

source("R/load_packages.R")
source("R/utils_helpers.R")

library(terra)
library(sf)
library(ggplot2)
library(scales)
library(ragg)

# --- Projection (Robinson)
robin <- "+proj=robin +lon_0=0 +datum=WGS84 +units=m +no_defs"

# --- Inputs
rs <- terra::rast("outputs/count_per_pixel_birdlife_plus_sdms.tif")  # Species at threat (n)
front_pct <- terra::rast("outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q3_pct.tif")  # 0–100
land <- get_world_latlon()  # WGS84 land polygons

# --- Mask land (ocean only)
rs <- terra::mask(rs, terra::vect(land), inverse = TRUE)

# --- Prepare raster df for plotting (keep only >0)
df <- as.data.frame(rs, xy = TRUE, na.rm = FALSE)
names(df) <- c("x", "y", "val")
df <- df[!is.na(df$val) & df$val > 0, ]

# --- Front raster: match grid + handle 0–360 lon if needed
# (front raster you showed is 0..360; ggplot tiles usually expect -180..180)
front_pct <- terra::rotate(front_pct)

# Resample to match rs grid (same resolution/extent)
front_pct_rs <- terra::resample(front_pct, rs, method = "bilinear")

# Threshold for "fronty" cells (your cut)
thr <- 50
front_is <- front_pct_rs >= thr

# Build shade raster: shade ONLY non-front areas
# NA over fronts (no shade), 1 elsewhere (shade)
shade <- terra::ifel(front_is, NA, 1)

shade_df <- as.data.frame(shade, xy = TRUE, na.rm = FALSE)
names(shade_df) <- c("x", "y", "shade")
shade_df <- shade_df[!is.na(shade_df$shade), ]

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

earth_outline <- st_sfc(st_linestring(ring), crs = 4326) |> st_transform(robin)

# --- Breaks
brks <- c(1, 5, 10, 20, 30, 40, 50, 60, 70, 80)

# --- Theme (minimal, map-style)
theme_map <- theme_void() +
  theme(
    panel.grid.major = element_line(color = "grey80", linewidth = 0.3),
    panel.grid.minor = element_blank(),
    axis.text  = element_text(color = "grey30", size = 9),
    axis.title = element_blank(),
    legend.title = element_text(size = 10),
    legend.text  = element_text(size = 9)
  )

# --- Plot
p4 <- ggplot() +
  # Species raster
  geom_tile(
    data = df,
    aes(x = x, y = y, fill = val),
    na.rm = TRUE
  ) +
  scale_fill_distiller(
    name      = "Species at threat (n)",
    palette   = "RdYlBu",
    breaks    = brks,
    labels    = scales::label_number(accuracy = 1),
    limits    = c(1, 74),
    oob       = scales::squish,
    na.value  = NA,
    direction = -1
  ) +
  # Shade-out non-fronts (transparent over fronts)
  geom_tile(
    data = shade_df,
    aes(x = x, y = y),
    fill = "grey70",
    alpha = 0.35,
    inherit.aes = FALSE
  ) +
  # Land + outline
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
  coord_sf(crs = robin, default_crs = st_crs(4326), expand = FALSE) +
  theme_map

ggsave(
  filename = "outputs/figures/exploratory/species_threat_fronts_AquaXBirdlife_v01c-3.png",
  plot = p4,
  width = 14, height = 7, units = "in",
  dpi = 300, bg = "white",
  device = ragg::agg_png
)
