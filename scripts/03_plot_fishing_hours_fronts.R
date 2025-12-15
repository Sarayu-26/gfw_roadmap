library(terra)
rs <- readRDS("outputs/combined_masked_sum.rds")
# plot(log10(rs)+1)
land <- get_world_latlon()
df <- as.data.frame(rs, xy = TRUE, na.rm = FALSE)
colnames(df) <- c("x", "y", "val")

ggplot() +
  geom_raster(
    data = df,
    aes(x = x, y = y, fill = val),
    na.rm = TRUE
  ) +
  scale_fill_viridis_c(
    trans = scales::log10_trans(),
    na.value = NA
  ) +
  geom_sf(
    data = front_poly,
    fill = NA,
    color = "red",
    linewidth = 0.2
  ) +
  geom_sf(
    data = land,
    fill = "grey20",
    color = "grey30",
    linewidth = 0.2
  ) +
  coord_sf(expand = FALSE)


#########################################################################################################################

rs <- readRDS("outputs/combined_masked_sum.rds")
front_poly <- readRDS("outputs/fsle_front_polygons/fsle_quartiles_1994_2022_Q3_pct_cut50.rds")
land <- get_world_latlon()
df <- as.data.frame(rs, xy = TRUE, na.rm = FALSE)
colnames(df) <- c("x", "y", "val")
df_sf <- st_as_sf(
  df,
  coords = c("x", "y"),
  crs = 4326,
  remove = FALSE
)

sf::sf_use_s2(FALSE)
inside <- st_intersects(df_sf, front_poly, sparse = FALSE)[, 1]
df_masked <- df_sf[inside, ]
df_masked2 <- df_sf[inside == FALSE, ]

log10p1 <- scales::trans_new(
  name = "log10p1",
  transform = function(x) log10(x + 1),
  inverse   = function(x) 10^x - 1
)
brks <- c(0, 1, 10, 100, 1e3, 1e4, 1e5, 1e6)


ggplot() +
  geom_tile(
    data = df_sf,
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
    data = front_poly,
    fill = NA,
    color = "red",
    linewidth = 0.3
  ) +
  geom_sf(
    data = land,
    fill = "grey20",
    color = "grey30",
    linewidth = 0.2
  ) +
  coord_sf(expand = FALSE)
