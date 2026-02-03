source("R/load_packages.R")
source("R/utils_helpers.R")

# =============================================================================
# 0) Packages
# =============================================================================
library(sf)            # spatial objects + geodesic operations
library(ggplot2)       # plotting
library(scales)        # transforms + label helpers
library(RColorBrewer)  # Brewer palettes (used via ggplot scale_*_distiller)
library(ggnewscale)    # multiple fill scales in one ggplot
library(ragg)

# =============================================================================
# 1) Projection settings
# =============================================================================
# Robinson projection (global maps, visually balanced)
robin <- "+proj=robin +lon_0=0 +datum=WGS84 +units=m +no_defs"

# =============================================================================
# 2) Inputs (raster + polygons + land)
# =============================================================================
# rs <- readRDS("outputs/combined_masked_sum_spsN.rds")  # raster (Species at threat (n))
rs <- rast("outputs/count_per_pixel_birdlife_plus_sdms.tif")  # raster (Species at threat (n)) -->> AquqX models + Birdlife
front_poly <- readRDS("outputs/fsle_front_polygons/fsle_quartiles_1994_2022_Q3_pct_cut50.rds")  # front hotspot polygons
land <- get_world_latlon()  # land polygons in lon/lat (WGS84)

# =============================================================================
# 3) Raster to data frame (lon/lat grid)
# =============================================================================
# Convert raster to a long table with lon/lat columns for ggplot tiles
df <- as.data.frame(rs, xy = TRUE, na.rm = FALSE)
colnames(df) <- c("x", "y", "val")  # val = Species at threat (n)

# keep only >0 (drop zeros and NAs)
df <- df[!is.na(df$val) & df$val > 0, ]

# =============================================================================
# 4) Spatial masking setup (points for point-in-polygon test)
# =============================================================================
# Convert raster grid cell centers to sf points for masking
df_sf <- st_as_sf(df, coords = c("x", "y"), crs = 4326, remove = FALSE)

# Use planar operations (avoid s2 loop-crossing errors for some geometries)
sf::sf_use_s2(FALSE)

# =============================================================================
# 5) Front polygons: version for masking (NO dateline wrapping)
# =============================================================================
# Make polygons valid, extract polygons only, and dissolve into one geometry
front_poly_mask <- st_make_valid(front_poly)
front_poly_mask <- st_collection_extract(front_poly_mask, "POLYGON")
front_poly_mask <- st_union(front_poly_mask)

# Identify grid cells inside front polygons (logical vector)
inside <- st_intersects(df_sf, front_poly_mask, sparse = FALSE)[, 1]

# Subset raster cells inside fronts
df_masked <- df[inside, ]

# =============================================================================
# 6) Optional zonal summaries (tropics vs temperate)
# =============================================================================

# subsets
df_all <- df
df_in  <- df[inside, ]
df_out <- df[!inside, ]

# latitude filters
is_trop <- function(d) d$y >= -23.5 & d$y <= 23.5

# temperate (combined)
is_temp <- function(d) abs(d$y) > 23.5 & abs(d$y) <= 60

# temperate split (north / south)
is_temp_north <- function(d) d$y > 23.5 & d$y <= 60
is_temp_south <- function(d) d$y < -23.5 & d$y >= -60

# global stats as a named vector
gstats <- function(v) {
  v <- v[!is.na(v)]
  c(
    n      = length(v),
    mean   = if (length(v)) mean(v) else NA_real_,
    median = if (length(v)) median(v) else NA_real_,
    sd     = if (length(v) > 1) sd(v) else NA_real_,
    min    = if (length(v)) min(v) else NA_real_,
    max    = if (length(v)) max(v) else NA_real_
  )
}

# ---- ALL pixels (df)
g_all_all        <- gstats(df_all$val)
g_all_trop       <- gstats(df_all$val[is_trop(df_all)])
g_all_temp       <- gstats(df_all$val[is_temp(df_all)])
g_all_temp_north <- gstats(df_all$val[is_temp_north(df_all)])
g_all_temp_south <- gstats(df_all$val[is_temp_south(df_all)])

# ---- INSIDE hotspots
g_in_all         <- gstats(df_in$val)
g_in_trop        <- gstats(df_in$val[is_trop(df_in)])
g_in_temp        <- gstats(df_in$val[is_temp(df_in)])
g_in_temp_north  <- gstats(df_in$val[is_temp_north(df_in)])
g_in_temp_south  <- gstats(df_in$val[is_temp_south(df_in)])

# ---- OUTSIDE hotspots
g_out_all        <- gstats(df_out$val)
g_out_trop       <- gstats(df_out$val[is_trop(df_out)])
g_out_temp       <- gstats(df_out$val[is_temp(df_out)])
g_out_temp_north <- gstats(df_out$val[is_temp_north(df_out)])
g_out_temp_south <- gstats(df_out$val[is_temp_south(df_out)])

# combine to table
out <- rbind(
  all_all            = g_all_all,
  all_trop           = g_all_trop,
  all_temp           = g_all_temp,
  all_temp_north     = g_all_temp_north,
  all_temp_south     = g_all_temp_south,
  inside_all         = g_in_all,
  inside_trop        = g_in_trop,
  inside_temp        = g_in_temp,
  inside_temp_north  = g_in_temp_north,
  inside_temp_south  = g_in_temp_south,
  outside_all        = g_out_all,
  outside_trop       = g_out_trop,
  outside_temp       = g_out_temp,
  outside_temp_north = g_out_temp_north,
  outside_temp_south = g_out_temp_south
)

out_df <- data.frame(
  subset = rownames(out),
  out,
  row.names = NULL,
  check.names = FALSE
)

write.csv(out_df, "outputs/summary_stats_spsN_v03.csv", row.names = FALSE)

# Quick check: how many raster cells fall inside polygons
message("masked rows: ", nrow(df_masked))

# =============================================================================
# 7) Front polygons: version for plotting (WITH dateline wrapping)
# =============================================================================
# Wrap dateline to avoid world-spanning polygon edges when projecting/plotting
front_poly_plot <- sf::st_wrap_dateline(
  front_poly_mask,
  options = c("WRAPDATELINE=YES", "DATELINEOFFSET=180"),
  quiet = TRUE
)

# =============================================================================
# 8) Legend breaks (Species at threat)
# =============================================================================
brks <- c(1, 5, 10, 20, 30, 40, 50, 60, 70, 80)

# =============================================================================
# 9) Build Robinson “earth outline” (true projection boundary)
# =============================================================================
# Construct a densified lon/lat ring around global extent, then project
lon <- seq(-180, 180, by = 0.5)
lat <- seq(-90, 90, by = 0.5)

top    <- cbind(lon,  90)
bottom <- cbind(rev(lon), -90)
left   <- cbind(rep(-180, length(lat)), lat)
right  <- cbind(rep( 180, length(lat)), rev(lat))

ring <- rbind(top, right, bottom, left, top)

earth_outline <- st_sfc(
  st_linestring(ring),
  crs = 4326
) |>
  st_transform(robin)

# =============================================================================
# 10) Plot theme (shared)
# =============================================================================
theme_map <- theme_void() +
  theme(
    panel.grid.major = element_line(color = "grey80", linewidth = 0.3),
    panel.grid.minor = element_blank(),
    panel.border = element_blank(),
    
    axis.text  = element_text(color = "grey30", size = 9),
    axis.title = element_blank(),
    
    legend.title = element_text(size = 10),
    legend.text  = element_text(size = 9),
    
    # Stack multiple legends vertically (needed for ggnewscale)
    legend.box = "vertical",
    
    # Increase separation between stacked legends
    legend.spacing.y = unit(50, "pt"),
    
    # Optional padding around the legend block
    legend.box.margin = margin(t = 0, r = 0, b = 0, l = 0)
  )

# =============================================================================
# 12) Figure v02: outside + inside (two fill scales)
# =============================================================================

# Raster cells outside fronts
df_masked_outside <- df[!inside, ]

p2 <- ggplot() +
  # ---------------------------------------------------------------------------
# Outside pixels first (muted, separate scale)
# ---------------------------------------------------------------------------
geom_tile(
  data = df_masked_outside,
  aes(x = x, y = y, fill = val),
  na.rm = TRUE,
  alpha = 0.35
) +
  scale_fill_distiller(
    name = "Outside front hotspot areas\nSpecies at threat (n)",
    palette   = "Greys",
    breaks    = brks,
    labels    = scales::label_number(accuracy = 1),
    limits    = c(1, 74),
    oob       = scales::squish,
    na.value  = NA,
    direction = 1
  ) +
  
  # Reset fill scale so the next tiles use an independent palette + legend
  ggnewscale::new_scale_fill() +
  
# ---------------------------------------------------------------------------
# Inside pixels (primary signal, separate scale)
# ---------------------------------------------------------------------------
geom_tile(
  data = df_masked,
  aes(x = x, y = y, fill = val),
  na.rm = TRUE
) +
  scale_fill_distiller(
    name = "Inside front hotspot areas\nSpecies at threat (n)",
    palette   = "RdYlBu", # YlOrRd
    breaks    = brks,
    labels    = scales::label_number(accuracy = 1),
    limits    = c(1, 74),
    oob       = scales::squish,
    na.value  = NA,
    direction = -1 # 1 for YlOrRd
  ) +
  
  # Front polygons
  geom_sf(
    data = front_poly_plot,
    fill = NA,
    color = "red",
    linewidth = 0.3,
    inherit.aes = FALSE
  ) +
  # Land mask
  geom_sf(
    data = land,
    fill = "grey20",
    color = "grey30",
    linewidth = 0.2,
    inherit.aes = FALSE
  ) +
  # Earth outline
  geom_sf(
    data = earth_outline,
    color = "grey50",
    linewidth = 1.0,
    inherit.aes = FALSE
  ) +
  # Robinson projection; default_crs ensures lon/lat tiles are projected correctly
  coord_sf(
    crs = robin,
    default_crs = st_crs(4326),
    expand = FALSE
  ) +
  theme_map

# ggsave(
#   filename = "outputs/figures/exploratory/species_threat_fronts_v02.pdf",
#   plot = p2, dpi = 400, width = 20, height = 10
# )

ggsave(
  filename = "outputs/figures/exploratory/species_threat_fronts_AquaXBirdlife_v01a.png",
  plot = p2,
  width = 14,
  height = 7,
  units = "in",
  dpi = 300,
  bg = "white",
  device = ragg::agg_png
)


# =============================================================================
# 13) Figure v03: outside + inside (two fill scales), outside greys squished at 20
# =============================================================================

# Rationale:
# Cap the OUTSIDE greyscale at 20 Species at threat (n) to increase visual gradation
# in low-value regions, while still showing all outside pixels by saturating
# values > 20 at the maximum grey tone.

brks_out <- c(1, 5, 10, 20)   # breaks for the outside (grey) scale

# Raster cells outside fronts
df_masked_outside <- df[!inside, ]

p3 <- ggplot() +
  # ---------------------------------------------------------------------------
# Outside pixels first (muted, separate scale)
# ---------------------------------------------------------------------------
geom_tile(
  data = df_masked_outside,
  aes(x = x, y = y, fill = val),
  na.rm = TRUE,
  alpha = 0.35
) +
  scale_fill_distiller(
    name = "Outside front hotspot areas\nSpecies at threat (n)",
    palette   = "Greys",
    breaks    = brks_out,
    labels    = scales::label_number(accuracy = 1),
    limits    = c(1, 20),
    oob       = scales::squish,   # keeps all pixels, saturates >20
    na.value  = NA,
    direction = 1
  ) +
  
  # Reset fill scale so the next tiles use an independent palette + legend
  ggnewscale::new_scale_fill() +
  
# ---------------------------------------------------------------------------
# Inside pixels (primary signal, separate scale)
# ---------------------------------------------------------------------------
geom_tile(
  data = df_masked,
  aes(x = x, y = y, fill = val),
  na.rm = TRUE
) +
  scale_fill_distiller(
    name = "Inside front hotspot areas\nSpecies at threat (n)",
    palette   = "YlOrRd",
    breaks    = brks,
    labels    = scales::label_number(accuracy = 1),
    limits    = c(1, 74),
    oob       = scales::squish,
    na.value  = NA,
    direction = 1
  ) +
  
  # Front polygons
  geom_sf(
    data = front_poly_plot,
    fill = NA,
    color = "red",
    linewidth = 0.3,
    inherit.aes = FALSE
  ) +
  # Land mask
  geom_sf(
    data = land,
    fill = "grey20",
    color = "grey30",
    linewidth = 0.2,
    inherit.aes = FALSE
  ) +
  # Earth outline
  geom_sf(
    data = earth_outline,
    color = "grey50",
    linewidth = 1.0,
    inherit.aes = FALSE
  ) +
  coord_sf(
    crs = robin,
    default_crs = st_crs(4326),
    expand = FALSE
  ) +
  theme_map

# ggsave(
#   filename = "outputs/figures/exploratory/species_threat_fronts_v03.pdf",
#   plot = p3, dpi = 400, width = 20, height = 10
# )

ggsave(
  filename = "outputs/figures/exploratory/species_threat_fronts_AquaXBirdlife_v01b.png",
  plot = p3,
  width = 14,
  height = 7,
  units = "in",
  dpi = 300,
  bg = "white",
  device = ragg::agg_png
)


# =============================================================================
# 14) Figure v04: single-layer raster (rs > 0 only), no inside vs outside
# =============================================================================
# Rationale:
# - We only plot raster cells with val > 0 (all zeros/NA dropped earlier), so the
#   map stays light and fast.
# - One fill scale only (no ggnewscale), using a distiller palette + your breaks.
# - Front polygons are shown as an outline for context (optional, keep/remove).
# =============================================================================

p4 <- ggplot() +
  
  # ---------------------------------------------------------------------------
# Raster signal only (rs > 0): Species at threat (n)
# ---------------------------------------------------------------------------
geom_tile(
  data = df,   # df already filtered to !is.na(val) & val > 0
  aes(x = x, y = y, fill = val),
  na.rm = TRUE
) +
  scale_fill_distiller(
    name = "Species at threat (n)",
    palette   = "RdYlBu",   # or "YlOrRd"
    breaks    = brks,
    labels    = scales::label_number(accuracy = 1),
    limits    = c(1, 74),
    oob       = scales::squish,
    na.value  = NA,
    direction = -1          # 1 for YlOrRd
  ) +
  
  # # Front polygons (optional)
  # geom_sf(
  #   data = front_poly_plot,
  #   fill = NA,
  #   color = "red",
  #   linewidth = 0.3,
  #   inherit.aes = FALSE
  # ) +
  
  # Land mask
  geom_sf(
    data = land,
    fill = "grey20",
    color = "grey30",
    linewidth = 0.2,
    inherit.aes = FALSE
  ) +
  
  # Earth outline
  geom_sf(
    data = earth_outline,
    color = "grey50",
    linewidth = 1.0,
    inherit.aes = FALSE
  ) +
  
  # Robinson projection; default_crs ensures lon/lat tiles are projected correctly
  coord_sf(
    crs = robin,
    default_crs = st_crs(4326),
    expand = FALSE
  ) +
  theme_map

ggsave(
  filename = "outputs/figures/exploratory/species_threat_fronts_AquaXBirdlife_v01c-2.png",
  plot = p4,
  width = 14,
  height = 7,
  units = "in",
  dpi = 300,
  bg = "white",
  device = ragg::agg_png
)

# Optional PDF export
# ggsave(
#   filename = "outputs/figures/exploratory/species_threat_fronts_v04_single.pdf",
#   plot = p4, dpi = 400, width = 20, height = 10
# )
