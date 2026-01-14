library(terra)
library(sf)
library(ggplot2)
library(patchwork)
source("R/earth_outline_robinson.R")

# =========================
# 1) Load + align rasters
# =========================
front_freq <- rast("outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q3_pct.tif")
front_freq <- terra::rotate(front_freq)
sdm_rsFF   <- rsFF

if (!compareGeom(front_freq, sdm_rsFF, stopOnError = FALSE)) {
  sdm_rsFF <- resample(sdm_rsFF, front_freq, method = "near")  # counts -> near
}

# ==========================================
# 2) Mask: keep only cells where BOTH > 0
# ==========================================
mask_both <- (front_freq > 0) & (sdm_rsFF > 0)

front_nz <- mask(front_freq, mask_both)
sdm_nz   <- mask(sdm_rsFF,  mask_both)

# ==========================================================
# 3) Quartiles on non-zero overlap + classify into 4 x 4 bins
# ==========================================================
qx <- as.numeric(quantile(values(front_nz), probs = c(0, .25, .5, .75, 1), na.rm = TRUE))
qy <- as.numeric(quantile(values(sdm_nz),   probs = c(0, .25, .5, .75, 1), na.rm = TRUE))

front_bin <- classify(front_nz, cbind(qx[-5], qx[-1], 1:4), include.lowest = TRUE, right = TRUE)
sdm_bin   <- classify(sdm_nz,   cbind(qy[-5], qy[-1], 1:4), include.lowest = TRUE, right = TRUE)

biv <- front_bin * 10 + sdm_bin
names(biv) <- "biv_class"

# =========================
# 4) Palette (16 classes)
# =========================
biv_cols_4x4 <- c(
  "11" = "#e8e8e8", "12" = "#d0d1e6", "13" = "#c7e9c0", "14" = "#fde0dd",
  "21" = "#b8d6be", "22" = "#9ebcda", "23" = "#74c476", "24" = "#fa9fb5",
  "31" = "#73ae80", "32" = "#6baed6", "33" = "#31a354", "34" = "#f768a1",
  "41" = "#2a5a5b", "42" = "#2171b5", "43" = "#006d2c", "44" = "#ae017e"
)

# ======================================================
# 5) Project raster + sf layers to Robinson for plotting
# ======================================================
robin_wkt <- sf::st_crs(robin)$wkt

biv_robin <- terra::project(biv, robin_wkt, method = "near")

# transform BOTH sf layers to Robinson
earth_poly_r <- sf::st_transform(earth_poly, robin)
land <- get_world_latlon()           # land polygons in lon/lat (WGS84)
land_r       <- sf::st_transform(land, robin)

# df for ggplot
df <- as.data.frame(biv_robin, xy = TRUE, na.rm = TRUE)
names(df)[3] <- "class"
df$class <- factor(df$class)

# ==================================
# 6) Map (use coord_sf + geom_tile)
# ==================================
p_map <- ggplot() +
  geom_sf(
    data = earth_poly_r,
    fill = "white",
    color = "grey15",
    linewidth = 0.3
  ) +
  geom_tile(
    data = df,
    aes(x = x, y = y, fill = class),
    inherit.aes = FALSE,
    linewidth = 0
  ) +
  scale_fill_manual(
    values = biv_cols_4x4,
    na.value = "transparent",
    drop = FALSE
  ) +
  geom_sf(
    data = land_r,
    fill = "grey20",
    color = "grey30",
    linewidth = 0.2,
    inherit.aes = FALSE
  ) +
  coord_sf(crs = robin, expand = FALSE) +
  theme_void() +
  theme(
    legend.position = "none",
    plot.background  = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )

# =========================
# 7) Legend (4 x 4 tiles)
# =========================
make_bivar_legend_4x4 <- function(biv_cols,
                                  x_title = "Species at threat (quartiles, >0 only)",
                                  y_title = "Front persistence (quartiles, >0 only)") {
  
  leg <- expand.grid(front_q = 1:4, sp_q = 1:4)
  leg$class <- as.character(10 * leg$front_q + leg$sp_q)
  
  leg$sp_q <- factor(leg$sp_q, levels = 1:4)
  leg$front_q <- factor(leg$front_q, levels = 1:4)
  
  ggplot(leg) +
    geom_tile(aes(x = sp_q, y = front_q, fill = class),
              color = "white", linewidth = 0.3) +
    scale_fill_manual(values = biv_cols, drop = FALSE) +
    scale_x_discrete(labels = c("Q1","Q2","Q3","Q4")) +
    scale_y_discrete(labels = c("Q1","Q2","Q3","Q4")) +
    labs(x = x_title, y = y_title) +
    coord_equal() +
    theme_minimal() +
    theme(
      legend.position = "none",
      panel.grid = element_blank(),
      axis.ticks = element_blank(),
      axis.title = element_text(size = 10),
      axis.text  = element_text(size = 9)
    )
}

p_leg <- make_bivar_legend_4x4(biv_cols_4x4)

# =========================
# 8) Combine + save
# =========================
plot_test <- p_map + p_leg + plot_layout(widths = c(4, 1))

# ggsave(
#   filename = "bivar_map_legend_4x4.pdf",
#   plot = plot_test,
#   width = 14,   # 30x15 is huge; this is cleaner for paper, scale as needed
#   height = 7,
#   bg = "white"
# )

ggsave(
  filename = "outputs/figures/exploratory/bivar_map_legend_4x4.png",
  plot = plot_test,
  width = 14,
  height = 7,
  dpi = 300,
  bg = "white"
)
