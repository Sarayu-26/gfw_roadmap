library(sf)
library(terra)
library(dplyr)
library(maps)


x <- st_read("/Users/ibrito/Downloads/species/BOTW_2025_subset_52.gpkg", quiet = TRUE)
# x <- st_read("/home/sandbox-sparc/z_BirdLife_roadmap/BOTW_2025_subset_52.gpkg", quiet = TRUE) # in the cluster
bird_sps <- unique(x$sci_name)

# x: your sf object with polygons + sci_name
# bird_sps: character vector of species names

if (!dir.exists("data/birdlife_rs_005")) {
  dir.create("data/birdlife_rs_005", recursive = TRUE)
}

# Template raster (0.05-degree)
tmpl_005 <- terra::rast(
  ncols = 7200, nrows = 3600,
  xmin = -180, xmax = 180,
  ymin = -90,  ymax = 90,
  crs  = "EPSG:4326"
)

# (Optional but often helpful) keep only needed cols once
# and ensure valid geometries if needed (can be expensive)
# x <- x |> dplyr::select(id, sisid, sci_name, geometry)

for (i in seq_along(bird_sps)) {
  
  sp <- bird_sps[i]
  fn <- gsub("\\s+", "_", sp)
  
  # subset sf (keeps geometry)
  x1_sf <- x %>%
    filter(sci_name == sp) %>%
    select(id, sisid, sci_name)
  
  # convert to SpatVector
  x1_v <- terra::vect(x1_sf)
  
  # rasterize: presence/absence (1 where species occurs)
  r <- terra::rasterize(
    x1_v, tmpl_005,
    field = 1,        # burn value
    background = NA    # ocean/elsewhere
  )
  
  # write raster to disk
  terra::writeRaster(
    r,
    filename = file.path("data/birdlife_rs_005", paste0(fn, "_005.tif")),
    overwrite = TRUE
  )
}

# plot(r)
# map("world", add = TRUE)
