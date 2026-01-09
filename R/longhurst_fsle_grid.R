# One-time step: rasterize Longhurst provinces to your FSLE grid (final output in 0..360)
# Output: a province-id GeoTIFF you can reuse in all runs

library(terra)
library(sf)

# ---- paths (edit these) ----
lgh_shp    <- "data/boundaries/longhurst/Longhurst_world_v4_2010.shp"
fsle_rs    <- "data/fronts_dynamical/dt_global_allsat_madt_fsle_2019-04.tif"
out_tif    <- "outputs/boundaries/longhurst_prov_id_fslegrid.tif"
out_lookup <- "outputs/boundaries/longhurst_prov_lookup.csv"

# dir.create(dirname(out_tif), recursive = TRUE, showWarnings = FALSE)

# ---- read ----
lgh <- st_read(lgh_shp, quiet = TRUE)

# IMPORTANT: use ONE FSLE layer as the true template (Pacific-centered 0..360)
tmpl0 <- rast(fsle_rs)[[1]]

# Create an internal rotated helper grid (-180..180) for wrapping/rasterizing
tmpl_rot <- terra::rotate(tmpl0)

# 1) match CRS to rotated template
tmpl_crs_sf <- st_crs(crs(tmpl_rot))
if (st_crs(lgh) != tmpl_crs_sf) {
  lgh <- st_transform(lgh, tmpl_crs_sf)
}

# 2) wrap dateline in -180..180 space (this is where it behaves best)
lgh <- st_wrap_dateline(
  lgh,
  options = c("WRAPDATELINE=YES", "DATELINEOFFSET=180"),
  quiet = TRUE
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
# write.csv(lookup, out_lookup, row.names = FALSE)

# ---- rasterize on rotated helper grid ----
v <- vect(lgh)

prov_rot <- tmpl_rot
values(prov_rot) <- NA_integer_

prov_rot <- rasterize(v, prov_rot, field = "prov_id", touches = TRUE)
names(prov_rot) <- "longhurst_prov_id"

# ---- rotate BACK to Pacific-centered 0..360 ----
prov_0 <- terra::rotate(prov_rot)

# ---- CRITICAL: force exact FSLE grid geometry WITHOUT resample ----
# 1) make sure prov covers tmpl0 (adds a thin border if needed)
prov_0 <- terra::extend(prov_0, tmpl0)

# 2) crop back to tmpl0 extent exactly
prov_0 <- terra::crop(prov_0, tmpl0)

# 3) force the same origin/alignment explicitly (should now match)
prov_0 <- terra::setValues(tmpl0, terra::values(prov_0))
names(prov_0) <- "longhurst_prov_id"

# ---- write outputs ----
writeRaster(prov_0, out_tif, overwrite = TRUE, wopt = list(datatype = "INT2S"))

# Sanity check: must now match the ORIGINAL FSLE template exactly
stopifnot(compareGeom(tmpl0, prov_0, stopOnError = FALSE))

message("[OK] Province raster written (0..360, aligned to FSLE): ", out_tif)
message("[OK] Lookup table written:                          ", out_lookup)
