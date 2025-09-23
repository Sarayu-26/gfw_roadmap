library(terra)
library(dplyr)
library(maps)

data <- read.csv("outputs/agg_cell_fishing_full.txt", header = TRUE, sep = "\t")
# pts <- vect(data, geom = c("lon", "lat"), crs = "EPSG:4326")
# r <- rast(pts, resolution = 0.01)
# r_fish <- rasterize(pts, r, field = "fishing_hours_sum", fun = "sum")
# 
# plot(r_fish)
# map("world", add = TRUE) 
# points(pts, col = "red", pch = 20)

data <- data %>% 
  dplyr::select(lon, lat, fishing_hours_sum)
r <- rast(data, type = "xyz", crs = "EPSG:4326")
plot(r)
