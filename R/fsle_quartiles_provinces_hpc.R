# This code was written by Isaac Brito-Morales (ibrito@conservation.org)
# Please do not distribute this code without permission.
# NO GUARANTEES THAT CODE IS CORRECT
# Caveat Emptor!

#' Compute persistence of FSLE quartile classes by province (region-relative)
#'
#' Like compute_fsle_quartile_persistence(), but Q1 and Q3 are computed per day
#' within each Longhurst province (or any province raster), not globally.
#'
#' @param fsle_dir Directory containing FSLE GeoTIFFs.
#' @param prov_raster Province raster aligned to FSLE grid (integer IDs, NA on land).
#'        Can be a filename (.tif) or a SpatRaster.
#' @param pattern Regex pattern to match FSLE files.
#' @param outdir Directory where outputs will be written.
#' @param prefix Prefix for output file names.
#' @param n_cores Number of workers for future::plan(multisession).
#' @param overwrite Logical, whether to overwrite existing output files.
#'
#' @return Invisibly returns a list with SpatRasters:
#'   - q1_pct, q2_pct, q3_pct and den_days (valid days per pixel)
#'
compute_fsle_quartile_persistence_by_province <- function(fsle_dir,
                                                          prov_raster,
                                                          pattern   = "^dt_global_allsat_madt_fsle_.*\\.tif$",
                                                          outdir    = "data_out",
                                                          prefix    = "fsle_quartile_persistence_prov",
                                                          n_cores   = 10,
                                                          overwrite = FALSE) {
  
  # --- Libraries ---
  stopifnot(requireNamespace("terra", quietly = TRUE))
  stopifnot(requireNamespace("future", quietly = TRUE))
  stopifnot(requireNamespace("future.apply", quietly = TRUE))
  
  library(terra)
  library(future)
  library(future.apply)
  
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  
  # --- Discover files ---
  fsle_files <- list.files(fsle_dir, pattern = pattern, full.names = TRUE)
  if (length(fsle_files) == 0L) {
    stop("No FSLE files found in '", fsle_dir, "' with pattern '", pattern, "'.")
  }
  message("[FSLE] Found ", length(fsle_files), " input file(s).")
  
  # --- Load province raster ---
  prov_r <- if (inherits(prov_raster, "SpatRaster")) prov_raster else terra::rast(prov_raster)
  
  # --- Terra options ---
  terra::terraOptions(progress = 0)
  terra::terraOptions(memfrac  = 0.8)
  
  # --- Directory for partial outputs ---
  partial_dir <- file.path(outdir, "partial_counts")
  dir.create(partial_dir, recursive = TRUE, showWarnings = FALSE)
  
  # --- Parallel plan ---
  message("[FSLE] Using n_cores = ", n_cores)
  old_plan <- future::plan()
  on.exit(future::plan(old_plan), add = TRUE)
  
  # CHANGE (Bellows/Linux): multicore avoids exporting terra externalptr objects
  future::plan(future::multicore, workers = n_cores)
  
  options(future.rng.onMisuse = "ignore")
  
  # --- Worker function: process a single file ---
  process_fsle_file <- function(fpath, idx, partial_dir, prov_r) {
    message("[Worker] Processing file: ", basename(fpath))
    
    r <- terra::rast(fpath)
    n_layers <- terra::nlyr(r)
    
    if (n_layers == 0L) {
      warning("[Worker] File has zero layers: ", fpath)
      return(list(q1_file = NA_character_, q2_file = NA_character_, q3_file = NA_character_,
                  den_file = NA_character_, file = fpath))
    }
    
    # Align province raster to this file's grid if needed (cheap if already aligned)
    tmpl <- r[[1]]
    if (!terra::compareGeom(tmpl, prov_r, stopOnError = FALSE)) {
      stop("[Worker] Province raster is not aligned to FSLE grid for file: ", fpath,
           "\nFix: rasterize provinces to the FSLE grid once, then reuse that tif.")
    }
    
    # Count rasters
    q1_count <- tmpl; terra::values(q1_count) <- 0L
    q2_count <- tmpl; terra::values(q2_count) <- 0L
    q3_count <- tmpl; terra::values(q3_count) <- 0L
    den_count <- tmpl; terra::values(den_count) <- 0L   # valid days per pixel
    
    # Province IDs present (constant across days)
    # We keep this local so workers do not fight global memory
    prov_vals_all <- terra::values(prov_r, mat = FALSE)
    prov_ids <- sort(unique(prov_vals_all[!is.na(prov_vals_all)]))
    
    if (length(prov_ids) == 0L) {
      stop("[Worker] Province raster has no non-NA IDs.")
    }
    
    for (i in seq_len(n_layers)) {
      day_r <- abs(r[[i]])
      
      # valid pixels are where both FSLE and province are non-NA
      valid_mask <- !is.na(day_r) & !is.na(prov_r)
      den_count <- den_count + terra::ifel(valid_mask, 1L, 0L)
      
      # Pull values for quantiles: two columns (fsle, prov_id)
      # This is the heavy step. It is correct, but can be slow for global 4 km.
      m <- terra::values(c(day_r, prov_r), mat = TRUE)
      if (is.null(m) || nrow(m) == 0L) next
      
      v_fsle <- m[, 1]
      v_prov <- m[, 2]
      
      ok <- !is.na(v_fsle) & !is.na(v_prov)
      if (!any(ok)) next
      
      v_fsle <- v_fsle[ok]
      v_prov <- v_prov[ok]
      
      # Per-province daily Q1 and Q3
      q1_vec <- rep(NA_real_, length(prov_ids))
      q3_vec <- rep(NA_real_, length(prov_ids))
      
      # loop provinces (54 for Longhurst, fine)
      for (p in seq_along(prov_ids)) {
        pid <- prov_ids[p]
        vv <- v_fsle[v_prov == pid]
        if (length(vv) < 10L) next  # skip tiny sample provinces for that day
        qs <- stats::quantile(vv, probs = c(0.25, 0.75), na.rm = TRUE, names = FALSE)
        q1_vec[p] <- qs[1]
        q3_vec[p] <- qs[2]
      }
      
      # Build per-pixel threshold rasters by mapping province id -> q1/q3
      # rcl format: 2 columns, from id to value
      rcl_q1 <- cbind(prov_ids, q1_vec)
      rcl_q3 <- cbind(prov_ids, q3_vec)
      
      q1_r <- terra::classify(prov_r, rcl = rcl_q1, others = NA, include.lowest = TRUE)
      q3_r <- terra::classify(prov_r, rcl = rcl_q3, others = NA, include.lowest = TRUE)
      
      # Class masks, strictly matching your definition
      c1 <- valid_mask & (day_r <= q1_r)
      c3 <- valid_mask & (day_r >  q3_r)
      c2 <- valid_mask & (day_r >  q1_r) & (day_r <= q3_r)
      
      q1_count <- q1_count + terra::ifel(c1, 1L, 0L)
      q2_count <- q2_count + terra::ifel(c2, 1L, 0L)
      q3_count <- q3_count + terra::ifel(c3, 1L, 0L)
    }
    
    # Write partial outputs
    q1_file  <- file.path(partial_dir, sprintf("partial_%04d_Q1.tif", idx))
    q2_file  <- file.path(partial_dir, sprintf("partial_%04d_Q2.tif", idx))
    q3_file  <- file.path(partial_dir, sprintf("partial_%04d_Q3.tif", idx))
    den_file <- file.path(partial_dir, sprintf("partial_%04d_DEN.tif", idx))
    
    terra::writeRaster(q1_count,  q1_file,  overwrite = TRUE)
    terra::writeRaster(q2_count,  q2_file,  overwrite = TRUE)
    terra::writeRaster(q3_count,  q3_file,  overwrite = TRUE)
    terra::writeRaster(den_count, den_file, overwrite = TRUE)
    
    list(q1_file = q1_file, q2_file = q2_file, q3_file = q3_file, den_file = den_file, file = fpath)
  }
  
  # --- Run in parallel over files ---
  res_list <- future.apply::future_lapply(
    seq_along(fsle_files),
    function(i) process_fsle_file(fsle_files[i], idx = i, partial_dir = partial_dir, prov_r = prov_r),
    future.seed = TRUE
  )
  
  # --- Keep only valid ---
  valid_res <- res_list[!vapply(res_list, function(x) is.na(x$q1_file), logical(1))]
  if (length(valid_res) == 0L) stop("[FSLE] No valid files were processed.")
  
  # --- Aggregate partial results (sum) ---
  message("[FSLE] Summing partial rasters...")
  
  q1_global  <- sum(terra::rast(vapply(valid_res, `[[`, "", "q1_file")))
  q2_global  <- sum(terra::rast(vapply(valid_res, `[[`, "", "q2_file")))
  q3_global  <- sum(terra::rast(vapply(valid_res, `[[`, "", "q3_file")))
  den_global <- sum(terra::rast(vapply(valid_res, `[[`, "", "den_file")))
  
  # --- Convert counts to percentages using per-pixel denominator ---
  message("[FSLE] Converting counts to percentages (per-pixel valid days)...")
  
  q1_pct <- terra::ifel(den_global > 0, (q1_global / den_global) * 100, NA)
  q2_pct <- terra::ifel(den_global > 0, (q2_global / den_global) * 100, NA)
  q3_pct <- terra::ifel(den_global > 0, (q3_global / den_global) * 100, NA)
  
  names(q1_pct) <- "fsle_q1_pct"
  names(q2_pct) <- "fsle_q2_pct"
  names(q3_pct) <- "fsle_q3_pct"
  names(den_global) <- "fsle_valid_days"
  
  # --- Write final outputs ---
  out_q1  <- file.path(outdir, paste0(prefix, "_Q1_pct.tif"))
  out_q2  <- file.path(outdir, paste0(prefix, "_Q2_pct.tif"))
  out_q3  <- file.path(outdir, paste0(prefix, "_Q3_pct.tif"))
  out_den <- file.path(outdir, paste0(prefix, "_valid_days.tif"))
  
  terra::writeRaster(q1_pct,  out_q1,  overwrite = overwrite)
  terra::writeRaster(q2_pct,  out_q2,  overwrite = overwrite)
  terra::writeRaster(q3_pct,  out_q3,  overwrite = overwrite)
  terra::writeRaster(den_global, out_den, overwrite = overwrite)
  
  message("[FSLE] Done.")
  message("[FSLE] Outputs:")
  message("  Q1:  ", out_q1)
  message("  Q2:  ", out_q2)
  message("  Q3:  ", out_q3)
  message("  DEN: ", out_den)
  
  invisible(list(
    q1_pct = q1_pct,
    q2_pct = q2_pct,
    q3_pct = q3_pct,
    den_days = den_global,
    files = fsle_files
  ))
}

# Example:
# compute_fsle_quartile_persistence_by_province(
#   fsle_dir     = "data/fronts_dynamical/",
#   prov_raster  = "data/boundaries/longhurst/longhurst_prov_id_fslegrid.tif",
#   outdir       = "data_rout/fsle_quartiles_longhurst",
#   prefix       = "fsle_quartiles_1994_2022_longhurst",
#   n_cores      = 10,
#   overwrite    = TRUE
# )