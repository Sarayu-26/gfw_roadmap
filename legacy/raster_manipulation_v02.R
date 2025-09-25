library(terra)
library(dplyr)
library(maps)


# GFW stuff ---------------------------------------------------------------

# reading the GFW data, all BUT gear
df <- read.csv("outputs/agg_cell_set_longlines_full.txt", sep = "\t") %>% 
  as_tibble() %>% 
  dplyr::select(-gear)
# from data frame to spatial raster, no aggregation at all 
rs_df <- terra::rast(df, type="xyz", , crs = "EPSG:4326")
plot(rs_df)



pts <- vect(df, geom = c("lon", "lat"), crs = "EPSG:4326")
rs_blank <- rast(res = 0.5, crs = "EPSG:4326")

rs_agg1 <- rasterize(
  pts, 
  rs_blank,
  field = "fishing_hours_sum", # variable, in this case is fihsing hours
  fun = "sum",      # aggregation function
  background = NA    # fill empty cells with 0
)
plot(log((rs_agg1)))
map("world", add = TRUE)

df2 <- read.csv("outputs/agg_cell_trawlers_full.txt", sep = "\t") %>% 
  as_tibble() %>% 
  dplyr::select(-gear)

pts2 <- vect(df2, geom = c("lon", "lat"), crs = "EPSG:4326")
rs_agg2 <- rasterize(
  pts2, 
  rs_blank,
  field = "fishing_hours_sum", # variable, in this case is fihsing hours
  fun = "sum",      # aggregation function
  background = NA    # fill empty cells with 0
)
plot(log((rs_agg2)))

rsFF <- terra::app(rs_agg1, rs_agg2, fun = sum, na.rm = TRUE) # set_longlines + trawlers
plot(log((rsFF)))

saveRDS(rsFF, "outputs/set_longlines-trawlers_05deg.rds")

# SDM stuff ---------------------------------------------------------------

load("data/FINAL_EMSDM_EMMEAN_SP_105797.Rdata")
sps <- FINALEMMEAN %>% 
  as_tibble() %>% 
  dplyr::select(x, y, Current)

# rs_sps <- terra::rast(sps, type="xyz", , crs = "EPSG:4326")
# plot(rs_sps)
# rs_sps[] <- ifelse(rs_sps[] > 0, 1, NA)

sps_105797 <- vect(sps, geom = c("x", "y"), crs = "EPSG:4326")
rs_105797 <- rasterize(
  sps_105797, 
  rs_blank,
  field = "Current", # variable, in this case is fihsing hours
  fun = "sum",      # aggregation function
  background = NA    # fill empty cells with 0
)
plot(rs_105797)
rs_105797[] <- ifelse(rs_105797[] > 0, 1, NA)

rs_FF <- mask(rsFF, rs_105797)
plot(log(rs_FF))
map("world", add = TRUE)

#===============================================================================
#making plot for orca (practice)

load("data/FINAL_EMSDM_EMMEAN_SP_137102.Rdata")
