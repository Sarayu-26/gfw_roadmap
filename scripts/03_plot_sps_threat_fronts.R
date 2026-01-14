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

# =============================================================================
# 1) Projection settings
# =============================================================================
# Robinson projection (global maps, visually balanced)
robin <- "+proj=robin +lon_0=0 +datum=WGS84 +units=m +no_defs"

# =============================================================================
# 2) Inputs (raster + polygons + land)
# =============================================================================
rs <- readRDS("outputs/combined_masked_sum.rds")  # raster (fishing hours or similar)
front_poly <- readRDS("outputs/fsle_front_polygons/fsle_quartiles_1994_2022_Q3_pct_cut50.rds")  # front hotspot polygons
land <- get_world_latlon()  # land polygons in lon/lat (WGS84)

# =============================================================================
# 3) Raster to data frame (lon/lat grid)
# =============================================================================
# Convert raster to a long table with lon/lat columns for ggplot tiles
df <- as.data.frame(rs, xy = TRUE, na.rm = FALSE)
colnames(df) <- c("x", "y", "val")  # x = lon, y = lat, val = fishing hours

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
# Note: this block assumes the pipe (%>%) is available in your session.
# You are already using dplyr::filter explicitly, but %>% comes from dplyr/magrittr.

# subsets
df_all <- df
df_in  <- df[inside, ]
df_out <- df[!inside, ]

# latitude filters
is_trop <- function(d) d$y >= -23.5 & d$y <= 23.5
is_temp <- function(d) abs(d$y) > 23.5 & abs(d$y) <= 60

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
g_all_all  <- gstats(df_all$val)
g_all_trop <- gstats(df_all$val[is_trop(df_all)])
g_all_temp <- gstats(df_all$val[is_temp(df_all)])

# ---- INSIDE hotspots
g_in_all   <- gstats(df_in$val)
g_in_trop  <- gstats(df_in$val[is_trop(df_in)])
g_in_temp  <- gstats(df_in$val[is_temp(df_in)])

# ---- OUTSIDE hotspots
g_out_all  <- gstats(df_out$val)
g_out_trop <- gstats(df_out$val[is_trop(df_out)])
g_out_temp <- gstats(df_out$val[is_temp(df_out)])

# combine to table
out <- rbind(
  all_all           = g_all_all,
  all_trop          = g_all_trop,
  all_temp          = g_all_temp,
  inside_all        = g_in_all,
  inside_trop       = g_in_trop,
  inside_temp       = g_in_temp,
  outside_all       = g_out_all,
  outside_trop      = g_out_trop,
  outside_temp      = g_out_temp
)

out_df <- data.frame(
  subset = rownames(out),
  out,
  row.names = NULL,
  check.names = FALSE
)

write.csv(out_df, "outputs/summary_stats.csv", row.names = FALSE)

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
# 8) Scale transform and legend breaks
# =============================================================================
# log10(1 + x) transform for fill scale (keeps zeros valid)
log10p1 <- scales::trans_new(
  name = "log10p1",
  transform = function(x) log10(x + 1),
  inverse   = function(x) 10^x - 1
)

# Breaks displayed in original units (before transform)
brks <- c(0, 10, 100, 1e3, 1e4, 1e5, 1e6)

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
# 11) Figure v01: inside-front pixels only
# =============================================================================
p1 <- ggplot() +
  # Raster tiles (only inside fronts)
  geom_tile(
    data = df_masked,
    aes(x = x, y = y, fill = val),
    na.rm = TRUE
  ) +
  # Inside scale (YlOrRd)
  scale_fill_distiller(
    name = expression(
      atop(
        "Inside front hotspot areas",
        log[10](1 + "fishing hours")
      )
    ),
    palette   = "YlOrRd",
    trans     = log10p1,
    breaks    = brks,
    labels    = scales::label_number(scale_cut = scales::cut_si("")),
    limits    = c(0, 1e6),          # adjust if you want a different cap
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
  # Robinson projection; default_crs ensures lon/lat tiles are projected correctly
  coord_sf(
    crs = robin,
    default_crs = st_crs(4326),
    expand = FALSE
  ) +
  theme_map

# print(p1)
ggsave(
  filename = "outputs/figures/exploratory/species_fishing_fronts_v01.png",
  plot = p1, dpi = 400, width = 20, height = 10
)

##########################################################################################

# =============================================================================
# 12) Figure v02: outside + inside (two fill scales)
# =============================================================================

# Color scale capped at 1e6 fishing hours.
# Values above this threshold represent <1% of grid cells (~0.77%),
# so censoring preserves contrast in the bulk of the data
# without materially affecting spatial patterns or comparisons.

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
    name = expression(
      atop(
        "Outside front hotspot areas",
        log[10](1 + "fishing hours")
      )
    ),
    palette   = "Greys",
    trans     = log10p1,
    breaks    = brks,
    labels    = scales::label_number(scale_cut = scales::cut_si("")),
    limits    = c(0, 1e6),
    oob       = scales::censor,
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
    name = expression(
      atop(
        "Inside front hotspot areas",
        log[10](1 + "fishing hours")
      )
    ),
    palette   = "YlOrRd",
    trans     = log10p1,
    breaks    = brks,
    labels    = scales::label_number(scale_cut = scales::cut_si("")),
    limits    = c(0, 1e6),
    oob       = scales::censor,
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
  # Robinson projection; default_crs ensures lon/lat tiles are projected correctly
  coord_sf(
    crs = robin,
    default_crs = st_crs(4326),
    expand = FALSE
  ) +
  theme_map

# print(p2)
ggsave(
  filename = "outputs/figures/exploratory/species_fishing_fronts_v02.pdf",
  plot = p2, dpi = 400, width = 20, height = 10
)


# =============================================================================
# 13) Figure v03: outside + inside (two fill scales), outside greys squished at 100
# =============================================================================

# Rationale:
# Cap the OUTSIDE greyscale at 100 fishing hours to increase visual gradation
# in low-effort regions, while still showing all outside pixels by saturating
# values > 100 at the maximum grey tone.

brks_out <- c(0, 1, 10, 100)   # breaks for the outside (grey) scale

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
    name = expression(
      atop(
        "Outside front hotspot areas",
        log[10](1 + "fishing hours")
      )
    ),
    palette   = "Greys",
    trans     = log10p1,
    breaks    = brks_out,
    labels    = scales::label_number(),
    limits    = c(0, 100),
    oob       = scales::squish,   # <-- this keeps all pixels, saturates >100
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
    name = expression(
      atop(
        "Inside front hotspot areas",
        log[10](1 + "fishing hours")
      )
    ),
    palette   = "YlOrRd",
    trans     = log10p1,
    breaks    = brks,
    labels    = scales::label_number(scale_cut = scales::cut_si("")),
    limits    = c(0, 1e6),
    oob       = scales::censor,
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

ggsave(
  filename = "outputs/figures/exploratory/species_fishing_fronts_v03.pdf",
  plot = p3, dpi = 400, width = 20, height = 10
)
