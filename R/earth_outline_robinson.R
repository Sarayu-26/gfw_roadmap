# ============================
# Build Robinson “earth outline”
# Author: Isaac Brito-Morales
# Email: ibrito@conservation.org
# ============================

source("R/utils_helpers.R")
source("R/load_packages.R")

earth_outline_robinson <- function(crs_out = robin, 
                                   lon_step = 0.5, 
                                   lat_step = 0.5) {
  
  # Construct a densified lon/lat ring around global extent
  lon <- seq(-180, 180, by = lon_step)
  lat <- seq(-90,  90,  by = lat_step)
  
  top    <- cbind(lon,  90)
  bottom <- cbind(rev(lon), -90)
  left   <- cbind(rep(-180, length(lat)), lat)
  right  <- cbind(rep( 180, length(lat)), rev(lat))
  
  ring <- rbind(top, right, bottom, left, top)
  
  earth_outline <- sf::st_sfc(
    sf::st_linestring(ring),
    crs = 4326
  ) |>
    sf::st_transform(crs_out)
  
  return(earth_outline)
}

# --- turn the earth_outline LINESTRING into a fillable POLYGON ---
earth_outline <- earth_outline_robinson()
coords <- sf::st_coordinates(earth_outline)
# keep only XY
xy <- coords[, c("X","Y"), drop = FALSE]
# force closure (polygon needs first point == last point)
if (!all(xy[1, ] == xy[nrow(xy), ])) {
  xy <- rbind(xy, xy[1, ])
}
earth_poly <- sf::st_sf(
  geometry = sf::st_sfc(sf::st_polygon(list(xy)), crs = sf::st_crs(earth_outline))
)
