# This code was written by Isaac Brito-Morales (ibrito@conservation.org)
# Please do not distribute this code without permission.
# NO GUARANTEES THAT CODE IS CORRECT
# Caveat Emptor!

#' Build fishing-effort rasters masked by seabird presence (BirdLife rasters + GFW gears)
#'
#' For each seabird presence raster (.tif; BirdLife-derived, already global 0.05-degree),
#' this function:
#'  1) extracts the Scientific Name from the raster filename (e.g., "Ardenna_gravis_005.tif"
#'     -> "Ardenna gravis"; the trailing "_005" is ignored)
#'  2) matches that Scientific Name to seabird_gfw_roadmap metadata (Scientific Name + GFWGearType)
#'  3) loads and sums the matching GFW gear rasters (agg_cell_<GEAR>.tif)
#'  4) masks the summed fishing effort raster by the seabird presence raster (non-NA cells)
#'  5) writes rs_FF to GeoTIFF named basename(bird_raster) with ".tif"
#'
#' Processing is parallelized across rasters using future.apply.
#'
#' @param bird_dir Directory containing BirdLife seabird presence rasters (.tif).
#' @param bird_pattern Regex pattern to match seabird rasters.
#' @param metadata_csv Path to seabird_gfw_roadmap.csv (must include Scientific Name and GFWGearType).
#' @param gfw_dir Directory containing GFW gear rasters (agg_cell_<GEAR>.tif).
#' @param gfw_pattern Regex pattern to match GFW gear rasters.
#' @param outdir Output directory where masked rs_FF GeoTIFFs will be written.
#' @param n_cores Number of workers for future::plan(multicore). Default 8.
#' @param overwrite Logical, whether to overwrite existing output files.
#' @param do_plot Logical, whether to plot log10(rs_FF) for each species (not recommended in parallel).
#' @param check_grid Logical, if TRUE, checks that input rasters match global 0.05-degree grid
#'   (extent/resolution/CRS). If mismatch is detected, it will stop (default TRUE).
#'
#' @return Invisibly returns a list with:
#'   - results: list of per-raster outputs (each element is either NULL or a list with rs_FF + out_file + sci_name)
#'   - skipped: character vector of Scientific Names skipped (missing metadata, missing/blank gear types, or no matching gear rasters)
#'   - bird_files: the seabird raster file paths processed
#'
#' @import terra
#' @import dplyr
#' @import tidyr
#' @import sf
#' @import future
#' @import future.apply
build_rs_FF_birdlife <- function(
    bird_dir      = "data/birdlife_rs_005/",
    bird_pattern  = ".*_005\\.tif$",
    metadata_csv  = "data/seabird_gfw_roadmap.csv",
    gfw_dir       = "data/gfw_rs",
    gfw_pattern   = "\\.tif$",
    outdir        = "data/rs_FF",
    n_cores       = 8,
    overwrite     = TRUE,
    do_plot       = FALSE,
    check_grid    = TRUE
) {
  
  # ---- packages (assume libraries are loaded, but keep safe checks) ----------
  if (!requireNamespace("terra", quietly = TRUE)) stop("Package 'terra' is required.")
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Package 'dplyr' is required.")
  if (!requireNamespace("future", quietly = TRUE)) stop("Package 'future' is required.")
  if (!requireNamespace("future.apply", quietly = TRUE)) stop("Package 'future.apply' is required.")
  
  # ---- I/O checks ------------------------------------------------------------
  if (!dir.exists(bird_dir)) stop("bird_dir not found: ", bird_dir)
  if (!file.exists(metadata_csv)) stop("metadata_csv not found: ", metadata_csv)
  if (!dir.exists(gfw_dir)) stop("gfw_dir not found: ", gfw_dir)
  if (!dir.exists(outdir)) dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  
  # ---- read metadata ---------------------------------------------------------
  metadata_sps <- read.csv(metadata_csv) |>
    dplyr::as_tibble()
  
  # ---- sanity check: expected columns ---------------------------------------
  if (!("Scientific.Name" %in% names(metadata_sps)) && !("Scientific Name" %in% names(metadata_sps))) {
    # Don't force a rename, just fail loudly so you catch it early.
    stop("metadata_csv must include a 'Scientific Name' column (or 'Scientific.Name' if read.csv made it syntactic).")
  }
  
  # Keep a consistent internal column name:
  if ("Scientific.Name" %in% names(metadata_sps) && !("Scientific Name" %in% names(metadata_sps))) {
    metadata_sps <- dplyr::rename(metadata_sps, `Scientific Name` = Scientific.Name)
  }
  
  if (!("GFWGearType" %in% names(metadata_sps))) {
    stop("metadata_csv must include a 'GFWGearType' column.")
  }
  
  # ---- list inputs -----------------------------------------------------------
  bird_files <- list.files(
    path = bird_dir,
    pattern = bird_pattern,
    full.names = TRUE,
    recursive = FALSE
  )
  
  if (length(bird_files) == 0) {
    stop("No seabird rasters found in ", bird_dir, " matching pattern: ", bird_pattern)
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
  
  # ---- helper: parse scientific name from file -------------------------------
  # Example: "Ardenna_gravis_005.tif" -> "Ardenna gravis"
  extract_sciname <- function(bird_file) {
    nm0 <- sub("\\.tif$", "", basename(bird_file), ignore.case = TRUE)
    nm0 <- sub("_005$", "", nm0)           # drop trailing resolution tag
    nm0 <- gsub("_+", " ", nm0)            # underscores to spaces
    trimws(nm0)
  }
  
  # ---- per-file worker -------------------------------------------------------
  process_one <- function(bird_file) {
    
    # output name: basename(bird_file) -> .tif (drop any extension, keep same base)
    out_name <- paste0(tools::file_path_sans_ext(basename(bird_file)), ".tif")
    out_file <- file.path(outdir, out_name)
    
    if (file.exists(out_file) && !isTRUE(overwrite)) {
      return(list(rs_FF = terra::rast(out_file),
                  out_file = out_file,
                  sci_name = NA_character_,
                  skipped = FALSE,
                  skip_reason = "exists"))
    }
    
    # Scientific name from filename
    sci <- extract_sciname(bird_file)
    if (is.na(sci) || sci == "") {
      return(list(rs_FF = NULL, out_file = out_file, sci_name = sci,
                  skipped = TRUE, skip_reason = "bad_scientific_name_parse"))
    }
    
    # read seabird raster
    bird_rs <- terra::rast(bird_file)
    
    # optional grid checks (fail early if not global 0.05-deg WGS84)
    if (isTRUE(check_grid)) {
      # Check resolution
      res_ok <- all(abs(terra::res(bird_rs) - c(0.05, 0.05)) < 1e-10)
      # Check extent
      ext_ok <- {
        e <- terra::ext(bird_rs)
        isTRUE(all(abs(c(e$xmin, e$xmax, e$ymin, e$ymax) - c(-180, 180, -90, 90)) < 1e-10))
      }
      # Check CRS (allow WKT variations, just require lon/lat + EPSG:4326 equivalent)
      crs_ok <- {
        cr <- terra::crs(bird_rs, describe = TRUE)
        # terra returns a list when describe=TRUE; try to use code if present
        if (is.list(cr) && "code" %in% names(cr)) {
          grepl("4326", as.character(cr$code))
        } else {
          grepl("4326", as.character(terra::crs(bird_rs)))
        }
      }
      
      if (!res_ok || !ext_ok || !crs_ok) {
        return(list(rs_FF = NULL, out_file = out_file, sci_name = sci,
                    skipped = TRUE, skip_reason = "bird_raster_grid_mismatch"))
      }
    }
    
    # metadata match
    info_species <- metadata_sps |>
      dplyr::filter(`Scientific Name` == sci)
    
    if (nrow(info_species) == 0) {
      return(list(rs_FF = NULL, out_file = out_file, sci_name = sci,
                  skipped = TRUE, skip_reason = "no_metadata"))
    }
    
    # gear types (handle NA/blank)
    gfw_gt_raw <- info_species$GFWGearType[1]
    if (is.na(gfw_gt_raw) || trimws(gfw_gt_raw) == "") {
      return(list(rs_FF = NULL, out_file = out_file, sci_name = sci,
                  skipped = TRUE, skip_reason = "missing_GFWGearType"))
    }
    
    # gear-type parsing (same logic as your SDM version)
    gtt <- gsub("\\s+", "", unique(info_species$GFWGearType))
    gtt <- sort(unlist(strsplit(gtt, "_")))
    
    files_true <- sort(gfw_rs[basename(gfw_rs) %in% paste0("agg_cell_", gtt, ".tif")])
    
    if (length(files_true) == 0) {
      return(list(rs_FF = NULL, out_file = out_file, sci_name = sci,
                  skipped = TRUE, skip_reason = "no_matching_gear_rasters"))
    }
    
    # sum fishing effort rasters
    rs_files_true_FF <- lapply(files_true, terra::rast)
    rs_files_true_FF <- terra::rast(rs_files_true_FF)
    rs_files_true_FF <- terra::app(rs_files_true_FF, fun = sum, na.rm = TRUE)
    
    # mask (keep effort where bird raster is non-NA)
    rs_FF <- terra::mask(rs_files_true_FF, bird_rs)
    
    # optional plot (avoid in parallel, but kept as arg)
    if (isTRUE(do_plot)) {
      plot(log10(rs_FF))
    }
    
    # write output
    terra::writeRaster(rs_FF, out_file, overwrite = TRUE)
    
    list(rs_FF = rs_FF, out_file = out_file, sci_name = sci,
         skipped = FALSE, skip_reason = NA_character_)
  }
  
  # ---- parallel plan + run ---------------------------------------------------
  old_plan <- future::plan()
  on.exit(future::plan(old_plan), add = TRUE)
  
  future::plan(future::multicore, workers = n_cores)
  
  results <- future.apply::future_lapply(
    X = bird_files,
    FUN = process_one,
    future.seed = TRUE
  )
  
  # ---- collect skipped -------------------------------------------------------
  skipped <- vapply(results, function(x) {
    if (isTRUE(x$skipped)) as.character(x$sci_name) else NA_character_
  }, FUN.VALUE = character(1))
  
  skipped <- skipped[!is.na(skipped)]
  
  invisible(list(
    results    = results,
    skipped    = skipped,
    bird_files = bird_files
  ))
}