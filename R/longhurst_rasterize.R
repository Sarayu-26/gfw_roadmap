# One-time step: rasterize Longhurst provinces to your FSLE grid
# Output: a province-id GeoTIFF you can reuse in all runs

library(terra)
library(sf)
library(lwgeom)

# ---- paths (edit these) ----
lgh_shp    <- "data/boundaries/longhurst/Longhurst_world_v4_2010.shp"
fsle_rs    <- "data/fronts_dynamical/dt_global_allsat_madt_fsle_2019-04.tif"  # any ONE FSLE file that matches the grid
out_tif    <- "outputs/boundaries/longhurst_prov_id_fslegrid.tif"
out_lookup <- "outputs/boundaries/longhurst_prov_lookup.csv"

# ---- read ----
lgh  <- st_read(lgh_shp, quiet = TRUE)
tmpl <- rast(fsle_rs)
tmpl <- terra::rotate(tmpl)


# 1) match CRS
tmpl_crs_sf <- sf::st_crs(terra::crs(tmpl))
if (sf::st_crs(lgh) != tmpl_crs_sf) {
  lgh <- sf::st_transform(lgh, tmpl_crs_sf)
}
# 2) wrap dateline (do this in lon/lat only, which FSLE usually is)
# this prevents those global "stitch" lines
lgh <- sf::st_wrap_dateline(
  lgh,
  options = c("WRAPDATELINE=YES", "DATELINEOFFSET=180")
)


# ---- create stable numeric IDs from ProvCode ----
code_levels <- sort(unique(as.character(lgh$ProvCode)))
lgh$prov_id <- match(as.character(lgh$ProvCode), code_levels)

lookup <- data.frame(
  prov_id   = seq_along(code_levels),
  ProvCode  = code_levels,
  ProvDescr = as.character(lgh$ProvDescr)[match(code_levels, as.character(lgh$ProvCode))],
  stringsAsFactors = FALSE
)

# ---- rasterize to FSLE grid ----
v <- vect(lgh)
prov_r <- tmpl
values(prov_r) <- NA_integer_

# touches=TRUE avoids small border gaps; set FALSE if you want strict coverage
prov_r <- rasterize(v, prov_r, field = "prov_id", touches = TRUE)
names(prov_r) <- "longhurst_prov_id"
prov_r <- terra::rotate(prov_r)

# ---- write outputs ----
writeRaster(prov_r, out_tif, overwrite = TRUE, wopt = list(datatype = "INT2S"))
write.csv(lookup, out_lookup, row.names = FALSE)

message("[OK] Province raster written: ", out_tif)
message("[OK] Lookup table written:   ", out_lookup)


