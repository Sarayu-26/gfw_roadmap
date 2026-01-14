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
#    (explicitly convert FALSE/0 to NA so mask() actually drops them)
# ==========================================
mask_both <- (front_freq > 0) & (sdm_rsFF > 0)

mask_keep <- mask_both
mask_keep[mask_both == 0] <- NA
mask_keep[mask_both == 1] <- 1

front_nz <- mask(front_freq, mask_keep)
sdm_nz   <- mask(sdm_rsFF,  mask_keep)

# ==========================================================
# 3) Tertiles on non-zero overlap
# ==========================================================
tx <- as.numeric(quantile(values(front_nz), probs = c(0, 1/3, 2/3, 1), na.rm = TRUE))
ty <- as.numeric(quantile(values(sdm_nz),   probs = c(0, 1/3, 2/3, 1), na.rm = TRUE))

# ==========================================================
# 3b) KEEP ONLY THE "UPPER TWO TERTILES" (T2 + T3) FOR BOTH
#     Collapse to 2x2 classes: (T2/T3) x (T2/T3)
# ==========================================================
# classify into tertiles (1..3)
front_bin <- classify(front_nz, cbind(tx[-4], tx[-1], 1:3),
                      include.lowest = TRUE, right = TRUE)

sdm_bin   <- classify(sdm_nz,   cbind(ty[-4], ty[-1], 1:3),
                      include.lowest = TRUE, right = TRUE)

# keep only bins 2 and 3 in BOTH layers
keep_upper <- (front_bin %in% c(2,3)) & (sdm_bin %in% c(2,3))

# recode 2->1 (T2), 3->2 (T3)
front_2 <- front_bin
sdm_2   <- sdm_bin

front_2[front_bin == 2] <- 1
front_2[front_bin == 3] <- 2

sdm_2[sdm_bin == 2] <- 1
sdm_2[sdm_bin == 3] <- 2

# combine to 2x2 classes: 11,12,21,22
biv_upper <- front_2 * 10 + sdm_2
biv_upper[!keep_upper] <- NA
names(biv_upper) <- "biv_class"

# =========================
# 4) Palette (2x2 = 4 classes)
# =========================
biv_cols_2x2 <- c(
  "11" = "#ad9ea5",  # Front T2, Species T2
  "12" = "#985356",  # Front T2, Species T3
  "21" = "#627f8c",  # Front T3, Species T2
  "22" = "#574249"   # Front T3, Species T3
)

# ======================================================
# 5) Project raster + sf layers to Robinson for plotting
# ======================================================
robin_wkt <- sf::st_crs(robin)$wkt

biv_robin <- terra::project(biv_upper, robin_wkt, method = "near")

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
    values = biv_cols_2x2,
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
# 7) Legend (2 x 2 tiles)
# =========================
make_bivar_legend_2x2 <- function(biv_cols,
                                  x_title = "Species at threat (T2 vs T3)",
                                  y_title = "Front persistence (T2 vs T3)") {
  
  leg <- expand.grid(front_b = 1:2, sp_b = 1:2)
  leg$class <- as.character(10 * leg$front_b + leg$sp_b)
  
  leg$sp_b    <- factor(leg$sp_b, levels = 1:2)
  leg$front_b <- factor(leg$front_b, levels = 1:2)
  
  ggplot(leg) +
    geom_tile(aes(x = sp_b, y = front_b, fill = class),
              color = "white", linewidth = 0.3) +
    scale_fill_manual(values = biv_cols, drop = FALSE) +
    scale_x_discrete(labels = c("T2","T3")) +
    scale_y_discrete(labels = c("T2","T3")) +
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

p_leg <- make_bivar_legend_2x2(biv_cols_2x2)

# =========================
# 8) Combine + save
# =========================
plot_test <- p_map + p_leg + plot_layout(widths = c(4, 1))

ggsave(
  filename = "outputs/figures/exploratory/bivar_map_upperT2T3_2x2.png",
  plot = plot_test,
  width = 14,
  height = 7,
  dpi = 300,
  bg = "white"
)
