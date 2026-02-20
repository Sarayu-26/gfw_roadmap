# ============================
# Utility Helpers
# Author: Isaac Brito-Morales
# Email: ibrito@conservation.org
# ============================

# ---- CRS Definitions ----
LatLon <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
moll   <- "+proj=moll +lon_0=0 +datum=WGS84 +units=m +no_defs"
robin  <- "+proj=robin +lon_0=0 +datum=WGS84 +units=m +no_defs"

# ---- Quick Functions ----
get_world_robin <- function(scale = "medium") {
  rnaturalearth::ne_countries(scale = scale, returnclass = "sf") |>
    sf::st_transform(crs = robin) |>
    sf::st_make_valid()
}

get_world_latlon <- function(scale = "medium", make_valid = FALSE) {
  x <- rnaturalearth::ne_countries(scale = scale, returnclass = "sf")
  if (make_valid) x <- sf::st_make_valid(x)
  x
}

# ---- Islands label helpers ----
# Note: keeping these as objects is convenient, but they are created at source time.
# If you ever want them lazy, we can convert them into functions.
islands_lbl <- tibble::tribble(
  ~name,          ~lon,    ~lat,
  "Europa",        40.367, -22.333,
  "Juan de Nova",  42.750, -17.050,
  "Tromelin",      54.517, -15.883
) |>
  sf::st_as_sf(coords = c("lon", "lat"), crs = 4326)

# prepare island label df (lon/lat + name)
islab_coords <- sf::st_coordinates(islands_lbl)
islands_lbl_df <- cbind(
  sf::st_drop_geometry(islands_lbl),
  lon = islab_coords[, 1],
  lat = islab_coords[, 2]
)

# ---- Color palettes ----
front_palette <- RColorBrewer::brewer.pal(9, "YlGnBu")

# ---- Units helpers ----
km_to_m <- function(km) units::set_units(km, "m")
m_to_km <- function(m)  units::set_units(m, "km")