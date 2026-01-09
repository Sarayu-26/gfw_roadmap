library(terra)

# -------------------------
# INPUTS (your real files)
# -------------------------
fsle_file <- "data/fronts_dynamical/dt_global_allsat_madt_fsle_2019-04.tif"
prov_file <- "outputs/boundaries/longhurst_prov_id_fslegrid.tif"

# outdir <- "reprex_global_vs_longhurst_2days"
# dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

# -------------------------
# READ: 2 daily layers from the monthly FSLE file
# -------------------------
r_all <- rast(fsle_file)
r <- r_all[[1:2]]                 # keep 2 days
rm(r_all)

prov <- rast(prov_file)

# Sanity: grids must match
if (!compareGeom(r[[1]], prov, stopOnError = FALSE)) {
  stop("Province raster is NOT aligned to FSLE grid. Recreate longhurst_prov_id_fslegrid.tif on the FSLE template grid.")
}

# -------------------------
# PART A) GLOBAL (no regionalization) for 2 days
# -------------------------
tmpl <- r[[1]]
g_q1 <- tmpl; values(g_q1) <- 0L
g_q2 <- tmpl; values(g_q2) <- 0L
g_q3 <- tmpl; values(g_q3) <- 0L
g_den <- tmpl; values(g_den) <- 0L

for (i in 1:nlyr(r)) {
  day_r <- abs(r[[i]])
  valid <- !is.na(day_r)
  g_den <- g_den + ifel(valid, 1L, 0L)
  
  vals <- values(day_r, mat = FALSE)
  if (all(is.na(vals))) next
  qs <- as.numeric(quantile(vals, probs = c(0.25, 0.75), na.rm = TRUE))
  if (any(is.na(qs))) next
  
  q1 <- qs[1]; q3 <- qs[2]
  
  c1 <- valid & (day_r <= q1)
  c3 <- valid & (day_r >  q3)
  c2 <- valid & (day_r >  q1) & (day_r <= q3)
  
  g_q1 <- g_q1 + ifel(c1, 1L, 0L)
  g_q2 <- g_q2 + ifel(c2, 1L, 0L)
  g_q3 <- g_q3 + ifel(c3, 1L, 0L)
}

g_q1_pct <- ifel(g_den > 0, (g_q1 / g_den) * 100, NA)
g_q2_pct <- ifel(g_den > 0, (g_q2 / g_den) * 100, NA)
g_q3_pct <- ifel(g_den > 0, (g_q3 / g_den) * 100, NA)

# -------------------------
# PART B) REGIONALIZED by Longhurst (province-relative) for the same 2 days
# -------------------------
p_q1 <- tmpl; values(p_q1) <- 0L
p_q2 <- tmpl; values(p_q2) <- 0L
p_q3 <- tmpl; values(p_q3) <- 0L
p_den <- tmpl; values(p_den) <- 0L

prov_vals <- values(prov, mat = FALSE)
prov_ids  <- sort(unique(prov_vals[!is.na(prov_vals)]))

for (i in 1:nlyr(r)) {
  day_r <- abs(r[[i]])
  
  valid <- !is.na(day_r) & !is.na(prov)
  p_den <- p_den + ifel(valid, 1L, 0L)
  
  # Pull FSLE + prov id together
  m <- values(c(day_r, prov), mat = TRUE)
  ok <- !is.na(m[,1]) & !is.na(m[,2])
  if (!any(ok)) next
  v  <- m[ok, 1]
  pr <- m[ok, 2]
  
  # Province-specific Q1/Q3 for this day
  q1v <- q3v <- rep(NA_real_, length(prov_ids))
  for (p in seq_along(prov_ids)) {
    vv <- v[pr == prov_ids[p]]
    if (length(vv) < 25L) next  # skip tiny sample sizes
    qs <- quantile(vv, c(0.25, 0.75), na.rm = TRUE, names = FALSE)
    q1v[p] <- qs[1]
    q3v[p] <- qs[2]
  }
  
  # Expand thresholds back to rasters
  q1_r <- classify(prov, cbind(prov_ids, q1v), others = NA)
  q3_r <- classify(prov, cbind(prov_ids, q3v), others = NA)
  
  # Class definitions (same as global, but thresholds are province-relative)
  c1 <- valid & (day_r <= q1_r)
  c3 <- valid & (day_r >  q3_r)
  c2 <- valid & (day_r >  q1_r) & (day_r <= q3_r)
  
  p_q1 <- p_q1 + ifel(c1, 1L, 0L)
  p_q2 <- p_q2 + ifel(c2, 1L, 0L)
  p_q3 <- p_q3 + ifel(c3, 1L, 0L)
}

p_q1_pct <- ifel(p_den > 0, (p_q1 / p_den) * 100, NA)
p_q2_pct <- ifel(p_den > 0, (p_q2 / p_den) * 100, NA)
p_q3_pct <- ifel(p_den > 0, (p_q3 / p_den) * 100, NA)

# -------------------------
# WRITE OUTPUTS (optional)
# -------------------------
writeRaster(g_q3_pct, file.path(outdir, "GLOBAL_Q3_pct_2days.tif"), overwrite = TRUE)
writeRaster(p_q3_pct, file.path(outdir, "LONGHURST_Q3_pct_2days.tif"), overwrite = TRUE)

# -------------------------
# VIZ (focus on Q3 difference)
# -------------------------
plot(g_q3_pct, main = "Global Q3 persistence (%) – 2 days")

pdf("reprex/LONGHURST_Q3_pct_2days.pdf", width = 20, height = 10)
plot(p_q3_pct, main = "Longhurst-relative Q3 persistence (%) – 2 days")
dev.off()


