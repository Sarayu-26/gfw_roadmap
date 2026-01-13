# This code was written by Isaac Brito-Morales (ibrito@conservation.org)
# Please do not distribute this code without permission.
# NO GUARANTEES THAT CODE IS CORRECT
# Caveat Emptor!

#' Build fishing-effort rasters masked by SDM presence (AquaMaps SDMs + GFW gears)
#'
#' For each SDM .Rdata file (AquaMaps-style), this function:
#'  1) extracts the AphiaID from the SDM filename (5th underscore-delimited field)
#'  2) matches that AphiaID to meta_species (AphiaID + GFWGearType)
#'  3) loads and sums the matching GFW gear rasters (agg_cell_<GEAR>.tif)
#'  4) converts SDM xyz data (x,y,Current) to a raster and resamples to 0.05-degree
#'  5) masks summed fishing effort by the SDM raster
#'  6) writes rs_FF to GeoTIFF named basename(sdm_file) with ".tif"
#'
#' Processing is parallelized across SDM files using future.apply.
#'
#' @param sdm_dir Directory containing SDM .Rdata files.
#' @param sdm_pattern Regex pattern to match SDM files.
#' @param metadata_csv Path to meta_species.csv (must include AphiaID and GFWGearType).
#' @param gfw_dir Directory containing GFW gear rasters (agg_cell_<GEAR>.tif).
#' @param gfw_pattern Regex pattern to match GFW gear rasters.
#' @param outdir Output directory where rs_FF GeoTIFFs will be written.
#' @param n_cores Number of workers for future::plan(multicore). Default 8.
#' @param overwrite Logical, whether to overwrite existing output files.
#' @param do_plot Logical, whether to plot log10(rs_FF) for each species (not recommended in parallel).
#'
#' @return Invisibly returns a list with:
#'   - results: list of per-SDM outputs (each element is either NULL or a list with rs_FF + out_file + AphiaID)
#'   - skipped: character vector of AphiaIDs skipped (missing metadata, missing/blank gear types, or no matching gear rasters)
#'   - sdm_files: the SDM file paths processed
#'
#' @import terra
#' @import dplyr
#' @import tidyr
#' @import sf
#' @import future
#' @import future.apply
build_rs_FF_aquax <- function(
    sdm_dir       = "data/aquax_sdms",
    sdm_pattern   = "SP.*\\.Rdata$",
    metadata_csv  = "data/meta_species.csv",
    gfw_dir       = "data/gfw_rs",
    gfw_pattern   = "\\.tif$",
    outdir        = "data/rs_FF",
    n_cores       = 8,
    overwrite     = TRUE,
    do_plot       = FALSE
) {
  
  # ---- packages (assume libraries are loaded, but keep safe checks) ----------
  if (!requireNamespace("terra", quietly = TRUE)) stop("Package 'terra' is required.")
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Package 'dplyr' is required.")
  if (!requireNamespace("future", quietly = TRUE)) stop("Package 'future' is required.")
  if (!requireNamespace("future.apply", quietly = TRUE)) stop("Package 'future.apply' is required.")
  
  # ---- I/O checks ------------------------------------------------------------
  if (!dir.exists(sdm_dir)) stop("sdm_dir not found: ", sdm_dir)
  if (!file.exists(metadata_csv)) stop("metadata_csv not found: ", metadata_csv)
  if (!dir.exists(gfw_dir)) stop("gfw_dir not found: ", gfw_dir)
  if (!dir.exists(outdir)) dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  
  # ---- read metadata ---------------------------------------------------------
  metadata_sps <- read.csv(metadata_csv) |>
    dplyr::as_tibble()
  
  # ---- helper: read SDM from .Rdata -----------------------------------------
  read_sdm <- function(file) {
    e <- new.env()
    load(file, envir = e)
    objname <- ls(e)[1]
    obj <- e[[objname]]
    attr(obj, "species_code") <- sub(".*SP_(\\d+)\\.Rdata$", "\\1", file)
    obj
  }
  
  # ---- helper: extract AphiaID (keep your approach) --------------------------
  extract_aphiaid <- function(sdm_file, idx = 5) {
    nm0 <- sub("\\.Rdata$", "", basename(sdm_file))
    parts <- strsplit(nm0, "_")[[1]]
    if (length(parts) < idx) return(NA_character_)
    parts[[idx]]
  }
  
  # ---- list inputs -----------------------------------------------------------
  sdm_files <- list.files(
    path = sdm_dir,
    pattern = sdm_pattern,
    full.names = TRUE,
    recursive = FALSE
  )
  
  if (length(sdm_files) == 0) {
    stop("No SDM files found in ", sdm_dir, " matching pattern: ", sdm_pattern)
  }
  
  gfw_rs <- list.files(
    path = gfw_dir,
    pattern = gfw_pattern,
    full.names = TRUE,
    recursive = FALSE
  )
  
  if (length(gfw_rs) == 0) {
    stop("No GFW rasters found in ", gfw_dir, " matching pattern: ", gfw_pattern)
  }
  
  # ---- template raster (0.05-degree) ----------------------------------------
  tmpl_005 <- terra::rast(
    ncols = 7200, nrows = 3600,
    xmin = -180, xmax = 180,
    ymin = -90,  ymax = 90,
    crs  = "EPSG:4326"
  )
  
  # ---- per-file worker -------------------------------------------------------
  process_one <- function(sdm_file) {
    
    # output name: basename(sdm_file) -> .tif
    out_name <- paste0(tools::file_path_sans_ext(basename(sdm_file)), ".tif")
    out_file <- file.path(outdir, out_name)
    
    if (file.exists(out_file) && !isTRUE(overwrite)) {
      return(list(rs_FF = terra::rast(out_file),
                  out_file = out_file,
                  AphiaID = NA_character_,
                  skipped = FALSE,
                  skip_reason = "exists"))
    }
    
    # AphiaID from filename
    nm <- extract_aphiaid(sdm_file, idx = 5)
    if (is.na(nm) || nm == "") {
      return(list(rs_FF = NULL, out_file = out_file, AphiaID = nm,
                  skipped = TRUE, skip_reason = "bad_AphiaID_parse"))
    }
    
    # metadata match
    info_species <- metadata_sps |>
      dplyr::filter(AphiaID == nm)
    
    if (nrow(info_species) == 0) {
      return(list(rs_FF = NULL, out_file = out_file, AphiaID = nm,
                  skipped = TRUE, skip_reason = "no_metadata"))
    }
    
    # gear types (handle NA/blank)
    gfw_gt_raw <- info_species$GFWGearType[1]
    if (is.na(gfw_gt_raw) || trimws(gfw_gt_raw) == "") {
      return(list(rs_FF = NULL, out_file = out_file, AphiaID = nm,
                  skipped = TRUE, skip_reason = "missing_GFWGearType"))
    }
    
    # your gear-type parsing
    gtt <- gsub("\\s+", "", unique(info_species$GFWGearType))
    gtt <- sort(unlist(strsplit(gtt, "_")))
    
    files_true <- sort(gfw_rs[basename(gfw_rs) %in% paste0("agg_cell_", gtt, ".tif")])
    
    if (length(files_true) == 0) {
      return(list(rs_FF = NULL, out_file = out_file, AphiaID = nm,
                  skipped = TRUE, skip_reason = "no_matching_gear_rasters"))
    }
    
    # sum fishing effort rasters
    rs_files_true_FF <- lapply(files_true, terra::rast)
    rs_files_true_FF <- terra::rast(rs_files_true_FF)
    rs_files_true_FF <- terra::app(rs_files_true_FF, fun = sum, na.rm = TRUE)
    
    # SDM -> raster -> resample to template
    sdm_data <- read_sdm(sdm_file) |>
      dplyr::select(x, y, Current) |>
      dplyr::as_tibble()
    
    sdm_rs <- terra::rast(sdm_data, type = "xyz", crs = "EPSG:4326")
    sdm_global <- terra::resample(sdm_rs, tmpl_005, method = "near")
    
    # mask
    rs_FF <- terra::mask(rs_files_true_FF, sdm_global)
    
    # optional plot (avoid in parallel, but kept as arg)
    if (isTRUE(do_plot)) {
      plot(log10(rs_FF))
    }
    
    # write output
    terra::writeRaster(rs_FF, out_file, overwrite = TRUE)
    
    list(rs_FF = rs_FF, out_file = out_file, AphiaID = nm,
         skipped = FALSE, skip_reason = NA_character_)
  }
  
  # ---- parallel plan + run ---------------------------------------------------
  old_plan <- future::plan()
  on.exit(future::plan(old_plan), add = TRUE)
  
  future::plan(future::multicore, workers = n_cores)
  
  results <- future.apply::future_lapply(
    X = sdm_files,
    FUN = process_one,
    future.seed = TRUE
  )
  
  # ---- collect skipped -------------------------------------------------------
  skipped <- vapply(results, function(x) {
    if (isTRUE(x$skipped)) as.character(x$AphiaID) else NA_character_
  }, FUN.VALUE = character(1))
  
  skipped <- skipped[!is.na(skipped)]
  
  invisible(list(
    results   = results,
    skipped   = skipped,
    sdm_files = sdm_files
  ))
}