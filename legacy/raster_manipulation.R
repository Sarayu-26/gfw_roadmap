library(terra)
library(dplyr)
library(maps)

data <- read.csv("outputs/agg_cell_drifting_longlines_full.txt", header = TRUE, sep = "\t")
# pts <- vect(data, geom = c("lon", "lat"), crs = "EPSG:4326")
# r <- rast(pts, resolution = 0.5)
# r_fish <- rasterize(pts, r, field = "fishing_hours_sum", fun = "sum")
# 
# plot(r_fish)
# map("world", add = TRUE) 
# points(pts, col = "red", pch = 20)

data <- data %>% 
  dplyr::select(lon, lat, fishing_hours_sum)
r <- rast(data, type = "xyz", crs = "EPSG:4326")
plot(r)


data <- data %>% 
  dplyr::select(lon, lat, fishing_hours_sum)
# Convert to SpatVector
pts <- vect(data, geom = c("lon", "lat"), crs = "EPSG:4326")
# Create an empty raster template at 0.5° resolution
r <- rast(res = 0.5, crs = "EPSG:4326")
# Rasterize with sum of fishing_hours
r_fishing <- rasterize(
  pts, r,
  field = "fishing_hours_sum",
  fun = "sum",      # aggregation function
  background = 0    # fill empty cells with 0
)
plot(log(r_fishing))


read_sdm <- function(file) {
  e <- new.env()
  load(file, envir = e)
  objname <- ls(e)[1]
  obj <- e[[objname]]
  attr(obj, "species_code") <- sub(".*SP_(\\d+)\\.Rdata$", "\\1", file)
  obj
}

sps_105797 <- read_sdm("data/FINAL_EMSDM_EMMEAN_SP_105797.Rdata")
sps_105797 <- sps_105797 %>% 
  dplyr::select(x, y, Current)
pts_sdm <- vect(sps_105797, geom = c("x", "y"), crs = "EPSG:4326")
# Create an empty raster template at 0.5° resolution
r_sdm <- rast(res = 0.5, crs = "EPSG:4326")
r_current <- rasterize(
  pts_sdm, r_sdm,
  field = "Current",
  fun = "sum",      # aggregation function
  background = 0    # fill empty cells with 0
)
r_current[] <- ifelse(r_current[] > 0, 1, NA)
plot(r_current)

r_fish_clipped <- mask(r_fishing, r_current)
plot(log(r_fish_clipped))
