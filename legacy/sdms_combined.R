
library(terra)
library(maps)
library(ggplot2)

list_tif <- list.files(path = "outputs/sdms_gfw", 
                       pattern = "*.tif", 
                       all.files = TRUE, 
                       full.names = TRUE, 
                       recursive = FALSE)

rs_sdms <- lapply(list_tif, function(x) rast(x))
rs_sdms <- rast(rs_sdms)
rsFF <- terra::app(rs_sdms, fun = function(x) sum(!is.na(x)))

plot(rsFF)
map("world", add = TRUE)