library(sf)
library(ggplot2)
library(scales)
library(RColorBrewer)
library(ggnewscale)

# Robinson projection
robin <- "+proj=robin +lon_0=0 +datum=WGS84 +units=m +no_defs"

# Load data
rs <- readRDS("outputs/combined_masked_sum.rds")
front_poly <- readRDS("outputs/fsle_front_polygons/fsle_quartiles_1994_2022_Q3_pct_cut50.rds")
land <- get_world_latlon()

# Raster to data frame (lon/lat)
df <- as.data.frame(rs, xy = TRUE, na.rm = FALSE)
colnames(df) <- c("x", "y", "val")

# Raster points as sf (for masking only)
df_sf <- st_as_sf(df, coords = c("x", "y"), crs = 4326, remove = FALSE)

sf::sf_use_s2(FALSE)

# ---- Polygon for masking (NO dateline wrap) ----
front_poly_mask <- st_make_valid(front_poly)
front_poly_mask <- st_collection_extract(front_poly_mask, "POLYGON")
front_poly_mask <- st_union(front_poly_mask)

inside <- st_intersects(df_sf, front_poly_mask, sparse = FALSE)[, 1]
df_masked <- df[inside, ]

df_trop <- df |>
  dplyr::filter(y >= -23.5, y <= 23.5, !is.na(val))
median(df_trop$val, na.rm = TRUE)
df_temp <- df %>% 
  dplyr::filter((abs(y) > 23.5 & abs(y) <= 60), !is.na(val))
median(df_temp$val, na.rm = TRUE)


message("masked rows: ", nrow(df_masked))

# ---- Polygon for plotting (WITH dateline wrap) ----
front_poly_plot <- sf::st_wrap_dateline(
  front_poly_mask,
  options = c("WRAPDATELINE=YES", "DATELINEOFFSET=180"),
  quiet = TRUE
)

# ---- log10(x + 1) transform ----
log10p1 <- scales::trans_new(
  name = "log10p1",
  transform = function(x) log10(x + 1),
  inverse   = function(x) 10^x - 1
)

brks <- c(0, 10, 100, 1e3, 1e4, 1e5, 1e6)

# ---- Build TRUE Robinson earth contour ----
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

theme_map <- theme_void() +
  theme(
    panel.grid.major = element_line(color = "grey80", linewidth = 0.3),
    panel.grid.minor = element_blank(),
    panel.border = element_blank(),
    
    axis.text  = element_text(color = "grey30", size = 9),
    axis.title = element_blank(),
    
    legend.title = element_text(size = 10),
    legend.text  = element_text(size = 9),
    
    # make sure legends are stacked
    legend.box = "vertical",
    
    # THIS is the key spacing between stacked legends
    legend.spacing.y = unit(50, "pt"),
    
    # optional extra breathing room around the whole legend block
    legend.box.margin = margin(t = 0, r = 0, b = 0, l = 0)
  )

# ---- Plot version 1 ----
p1 <- ggplot() +
  geom_tile(
    data = df_masked,
    aes(x = x, y = y, fill = val),
    na.rm = TRUE
  ) +
  scale_fill_distiller(
    name      = expression(log[10](1 + "fishing hours")),
    palette   = "YlOrRd",
    trans     = log10p1,
    breaks    = brks,
    labels    = scales::label_number(scale_cut = scales::cut_si("")),
    limits    = c(0, 1e6),          # adjust if you want a different cap
    oob       = scales::squish,
    na.value  = NA,
    direction = 1
  ) +
  geom_sf(
    data = front_poly_plot,
    fill = NA,
    color = "red",
    linewidth = 0.3,
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
    default_crs = st_crs(4326),
    expand = FALSE
  ) +
  theme_map

print(p1)

ggsave(filename = "outputs/figures/exploratory/species_fishing_fronts_v01.pdf", plot = p1, dpi = 400, width = 20, height = 10)




##########################################################################################

# Color scale capped at 1e6 fishing hours.
# Values above this threshold represent <1% of grid cells (~0.77%),
# so censoring preserves contrast in the bulk of the data
# without materially affecting spatial patterns or comparisons.

df_masked_outside <- df[!inside, ]
p2 <- ggplot() +
  # Outside pixels first (muted)
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
  
  ggnewscale::new_scale_fill() +
  
  # Inside pixels (your original scale)
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
  
  geom_sf(
    data = front_poly_plot,
    fill = NA,
    color = "red",
    linewidth = 0.3,
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
    default_crs = st_crs(4326),
    expand = FALSE
  ) +
  theme_map

# print(p2)
ggsave(filename = "outputs/figures/exploratory/species_fishing_fronts_v02.pdf", plot = p2, dpi = 400, width = 20, height = 10)

