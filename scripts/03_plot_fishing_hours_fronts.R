library(sf)
library(ggplot2)
library(scales)

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

# ---- Plot ----
ggplot() +
  geom_tile(
    data = df_masked,
    aes(x = x, y = y, fill = val),
    na.rm = TRUE
  ) +
  scale_fill_viridis_c(
    name     = "log10 + 1 fishing hours",
    trans    = log10p1,
    breaks   = brks,
    labels   = scales::label_number(scale_cut = scales::cut_si("")),
    na.value = NA
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
  theme_void() +
  theme(
    panel.grid = element_blank(),
    panel.border = element_blank()
  )