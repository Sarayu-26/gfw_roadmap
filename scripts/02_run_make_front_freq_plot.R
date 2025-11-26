source("R/utils_helpers.R")
source("R/make_front_polygon.R")


# frontQ3 <- make_front_polygon(rs_dir = "outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q3_pct.tif", 
#                    cutoff = 50, 
#                    outdir = "outputs/fsle_front_polygons/",
#                    outfile = NULL,  
#                    rotate_raster = TRUE)
# frontQ2 <- make_front_polygon(rs_dir = "outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q2_pct.tif", 
#                               cutoff = 50, 
#                               outdir = "outputs/fsle_front_polygons/",
#                               outfile = NULL,  
#                               rotate_raster = TRUE)

# -----------------------------------------------------------------------------
# ---- 00) Parameters ---------------------------------------------------------
# -----------------------------------------------------------------------------

params <- list(
  rds_path  = "outputs/fsle_front_polygons/fsle_quartiles_1994_2022_Q2_pct_cut50.rds",
  # out_pdf   = "outputs/figures/exploratory/front_global_cut50_Q3.pdf",
  out_png   = "outputs/figures/exploratory/front_global_cut50_Q2.png",
  dpi       = 600,
  width_in  = 20,
  height_in = 10
)

# read front polygon (generic, not hard-coded to Q3)
front_poly <- readRDS(params$rds_path)

p_front <- ggplot() +
# ---------------------------------------------------------------------------
# 1) Front polygon watermark-style diagonal hatch  (first layer)
# ---------------------------------------------------------------------------
geom_sf_pattern(
  data   = front_poly,
  aes(),
  pattern          = "stripe",
  fill             = NA,
  color            = NA,
  pattern_fill     = "steelblue2",
  pattern_colour   = "grey30",
  pattern_alpha    = 0.3,
  pattern_angle    = 45,
  pattern_density  = 1,
  pattern_spacing  = 0.006,
  pattern_size     = 0.10
) +
  
  # ---------------------------------------------------------------------------
# 2) Basemap OVER the pattern layer
# ---------------------------------------------------------------------------
geom_sf(
  data  = land_sf_lat,
  fill  = "grey20",
  color = "grey30",
  linewidth = 0.2
) +
  
  # ---------------------------------------------------------------------------
# 3) Viewport
# ---------------------------------------------------------------------------
coord_sf(
  xlim = c(-180, 180),
  ylim = c(-90, 90),
  expand = FALSE
) +
  
  # ---------------------------------------------------------------------------
# 4) Labels and theme
# ---------------------------------------------------------------------------
labs(title = "", x = "", y = "") +
  theme_minimal(base_size = 13) +
  theme(
    plot.background  = element_rect(fill = "white", colour = NA),
    panel.background = element_rect(fill = "white", colour = NA),
    panel.grid       = element_blank(),
    axis.text        = element_text(color = "grey70"),
    axis.ticks       = element_line(color = "grey50"),
    legend.position  = "none",
    legend.title     = element_text(
      hjust = 0,
      color = "black",
      size = 9,
      face = "bold"
    ),
    legend.text      = element_text(
      color = "black",
      size = 9
    ),
    panel.border     = element_rect(color = "black", fill = NA, linewidth = 0.6)
  )

# -----------------------------------------------------------------------------
# ---- 02) Save figure: front map --------------------------------------------
# -----------------------------------------------------------------------------

ggsave(
  filename = params$out_png,
  plot     = p_front,
  width    = params$width_in,
  height   = params$height_in,
  dpi      = params$dpi
)

# ggsave(
#   filename = params$out_pdf,
#   plot     = p_front,
#   width    = params$width_in,
#   height   = params$height_in,
#   dpi      = params$dpi,
#   device   = cairo_pdf
# )
