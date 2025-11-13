# This code was written by Isaac Brito-Morales (ibrito@conservation.org)
# Please do not distribute this code without permission.
# NO GUARANTEES THAT CODE IS CORRECT
# Caveat Emptor!

#' Compute persistence of FSLE quartile classes using terra and future
#'
#' This function reads a set of daily or monthly FSLE rasters (global, 4 km),
#' classifies each daily layer into three quartile-based classes:
#'   1 = values <= Q1
#'   2 = Q1 < values <= Q3
#'   3 = values > Q3
#'
#' It then accumulates, for each cell, how many days it spent in each class,
#' and finally converts those counts into percentages.
#'
#' The computation is parallelized over files using future.apply with
#' plan(multisession, workers = n_cores).
#'
#' @param fsle_dir Directory containing FSLE GeoTIFFs.
#' @param pattern Regex pattern to match FSLE files.
#' @param outdir Directory where outputs will be written.
#' @param prefix Prefix for output file names.
#' @param n_cores Number of workers for future::plan(multisession).
#' @param overwrite Logical, whether to overwrite existing output files.
#'
#' @return Invisibly returns a list with SpatRasters:
#'   - q1_pct: percent of days in Q1
#'   - q2_pct: percent of days in Q2
#'   - q3_pct: percent of days in Q3
#'
#' @import terra
#' @import future
#' @import future.apply
#'
compute_fsle_quartile_persistence <- function(fsle_dir,
                                              pattern   = "^dt_global_allsat_madt_fsle_.*\\.tif$",
                                              outdir    = "data_out",
                                              prefix    = "fsle_quartile_persistence",
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
  
  # --- Terra options ---
  terra::terraOptions(progress = 0)   # set to 1 if you want progress
  terra::terraOptions(memfrac  = 0.8)
  
  # --- Directory for partial outputs ---
  partial_dir <- file.path(outdir, "partial_counts")
  dir.create(partial_dir, recursive = TRUE, showWarnings = FALSE)
  
  # --- Parallel plan ---
  message("[FSLE] Using n_cores = ", n_cores)
  future::plan(future::multisession, workers = n_cores)
  
  # Ensure future does not complain about RNG
  options(future.rng.onMisuse = "ignore")
  
  # --- Worker function: process a single file ---
  process_fsle_file <- function(fpath, idx, partial_dir) {
    message("[Worker] Processing file: ", basename(fpath))
    
    r <- terra::rast(fpath)  # may have multiple daily layers
    
    n_layers <- terra::nlyr(r)
    if (n_layers == 0L) {
      warning("[Worker] File has zero layers: ", fpath)
      return(list(
        q1_file = NA_character_,
        q2_file = NA_character_,
        q3_file = NA_character_,
        n_days  = 0L,
        file    = fpath
      ))
    }
    
    # Template for count rasters: use first layer geometry
    tmpl <- r[[1]]
    
    q1_count <- tmpl
    q2_count <- tmpl
    q3_count <- tmpl
    
    # Initialize with zeros
    terra::values(q1_count) <- 0L
    terra::values(q2_count) <- 0L
    terra::values(q3_count) <- 0L
    
    # Loop over daily layers inside this file
    for (i in seq_len(n_layers)) {
      day_r <- r[[i]]
      day_r <- abs(day_r)  # use absolute FSLE (ignore sign)
      
      # Compute 25th and 75th percentiles for this day
      qs <- terra::quantile(day_r, probs = c(0.25, 0.75), na.rm = TRUE)
      qs_num <- as.numeric(qs)  # length 2: Q1, Q3
      
      # If quantiles are NA (e.g. all NA layer), skip
      if (any(is.na(qs_num))) {
        next
      }
      
      q1 <- qs_num[1]
      q3 <- qs_num[2]
      
      # Reclassify to 1, 2, 3 based on quartiles
      rcl <- matrix(
        c(-Inf, q1, 1,
          q1,  q3, 2,
          q3,  Inf, 3),
        ncol = 3,
        byrow = TRUE
      )
      
      day_class <- terra::classify(day_r, rcl = rcl, include.lowest = TRUE, right = TRUE)
      
      # Update counts
      q1_count <- q1_count + terra::ifel(day_class == 1, 1L, 0L)
      q2_count <- q2_count + terra::ifel(day_class == 2, 1L, 0L)
      q3_count <- q3_count + terra::ifel(day_class == 3, 1L, 0L)
    }
    
    # Write partial outputs for this file
    q1_file <- file.path(partial_dir, sprintf("partial_%04d_Q1.tif", idx))
    q2_file <- file.path(partial_dir, sprintf("partial_%04d_Q2.tif", idx))
    q3_file <- file.path(partial_dir, sprintf("partial_%04d_Q3.tif", idx))
    
    terra::writeRaster(q1_count, q1_file, overwrite = TRUE)
    terra::writeRaster(q2_count, q2_file, overwrite = TRUE)
    terra::writeRaster(q3_count, q3_file, overwrite = TRUE)
    
    list(
      q1_file = q1_file,
      q2_file = q2_file,
      q3_file = q3_file,
      n_days  = n_layers,
      file    = fpath
    )
  }
  
  # --- Run in parallel over files ---
  res_list <- future.apply::future_lapply(
    seq_along(fsle_files),
    function(i) process_fsle_file(fsle_files[i], idx = i, partial_dir = partial_dir),
    future.seed = TRUE
  )
  
  # --- Aggregate partial results ---
  valid_res <- res_list[!vapply(res_list, function(x) x$n_days == 0L, logical(1))]
  
  if (length(valid_res) == 0L) {
    stop("[FSLE] No valid layers were processed. Check your input.")
  }
  
  total_days <- sum(vapply(valid_res, function(x) x$n_days, integer(1)))
  message("[FSLE] Total days processed across all files: ", total_days)
  
  # Sum partial rasters for each class
  message("[FSLE] Summing partial Q1 rasters...")
  q1_global <- terra::rast(valid_res[[1]]$q1_file)
  if (length(valid_res) > 1L) {
    for (k in 2:length(valid_res)) {
      q1_global <- q1_global + terra::rast(valid_res[[k]]$q1_file)
    }
  }
  
  message("[FSLE] Summing partial Q2 rasters...")
  q2_global <- terra::rast(valid_res[[1]]$q2_file)
  if (length(valid_res) > 1L) {
    for (k in 2:length(valid_res)) {
      q2_global <- q2_global + terra::rast(valid_res[[k]]$q2_file)
    }
  }
  
  message("[FSLE] Summing partial Q3 rasters...")
  q3_global <- terra::rast(valid_res[[1]]$q3_file)
  if (length(valid_res) > 1L) {
    for (k in 2:length(valid_res)) {
      q3_global <- q3_global + terra::rast(valid_res[[k]]$q3_file)
    }
  }
  
  # --- Convert counts to percentages ---
  message("[FSLE] Converting counts to percentages...")
  
  q1_pct <- (q1_global / total_days) * 100
  q2_pct <- (q2_global / total_days) * 100
  q3_pct <- (q3_global / total_days) * 100
  
  names(q1_pct) <- "fsle_q1_pct"
  names(q2_pct) <- "fsle_q2_pct"
  names(q3_pct) <- "fsle_q3_pct"
  
  # --- Write final outputs ---
  out_q1 <- file.path(outdir, paste0(prefix, "_Q1_pct.tif"))
  out_q2 <- file.path(outdir, paste0(prefix, "_Q2_pct.tif"))
  out_q3 <- file.path(outdir, paste0(prefix, "_Q3_pct.tif"))
  
  terra::writeRaster(q1_pct, out_q1, overwrite = overwrite)
  terra::writeRaster(q2_pct, out_q2, overwrite = overwrite)
  terra::writeRaster(q3_pct, out_q3, overwrite = overwrite)
  
  message("[FSLE] Done.")
  message("[FSLE] Outputs:")
  message("  Q1: ", out_q1)
  message("  Q2: ", out_q2)
  message("  Q3: ", out_q3)
  
  invisible(list(
    q1_pct = q1_pct,
    q2_pct = q2_pct,
    q3_pct = q3_pct,
    total_days = total_days,
    files = fsle_files
  ))
}

## Example call (to be run inside your project, adjust paths)
# compute_fsle_quartile_persistence(
#   fsle_dir = "/scratch/fsle_global_daily",
#   pattern  = "^dt_global_allsat_madt_fsle_.*\\.tif$",
#   outdir   = "data_rout/fsle_quartiles_global",
#   prefix   = "fsle_quartiles_1994_2022",
#   n_cores  = 10,
#   overwrite = TRUE
# )