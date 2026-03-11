#' Create dissolved polygon mask from front raster using terra
#'
#' This function reads a front raster, applies a threshold, converts the result
#' to a binary mask, optionally rotates it from 0-360 to -180-180 when needed,
#' fixes slightly invalid lon/lat extents when present, polygonizes it in terra,
#' dissolves it into a single polygon, and writes the result as an RDS file
#' containing the sf object.
#'
#' By default, cutoff is interpreted in normalized units (0-1), so:
#'   - cutoff = 0.5 means 50% of the raster scale
#'   - for a 0-100 raster, that becomes 50
#'   - for a 0-1 raster, that stays 0.5
#'
#' @param rs_dir Character. Path to raster file.
#' @param cutoff Numeric. Threshold value. Default = 0.5.
#' @param cutoff_type Character. Either "normalized" or "absolute".
#'   - "normalized": cutoff is interpreted relative to raster range
#'   - "absolute": cutoff is used directly
#' @param outdir Character. Directory where the output polygon file will be stored.
#' @param outfile Optional character. Name of output file. If not provided, a name
#'   will be auto-generated based on the input raster name and effective cutoff.
#' @param rotate_raster Logical or NULL. If TRUE/FALSE, force behavior. If NULL,
#'   function decides automatically from raster extent. Default = NULL.
#' @param fix_extent Logical. If TRUE, clamps slightly invalid lon/lat extents
#'   to valid bounds when needed. Default = TRUE.
#' @param verbose Logical. If TRUE, print messages. Default = TRUE.
#'
#' @return A list containing:
#'   - terra: the dissolved terra polygon object
#'   - sf: the dissolved polygon as an sf object
#'   - rds_path: the RDS output file path
#'   - cutoff_input: the user-provided cutoff
#'   - cutoff_type: how cutoff was interpreted
#'   - cutoff_effective: the actual threshold used on the raster
#'   - rotated: whether raster was rotated
#'   - extent_fixed: whether extent was adjusted
#'
#' @export
make_front_polygon <- function(rs_dir,
                               cutoff = 0.5,
                               cutoff_type = c("normalized", "absolute"),
                               outdir,
                               outfile = NULL,
                               rotate_raster = NULL,
                               fix_extent = TRUE,
                               verbose = TRUE) {
  
  cutoff_type <- match.arg(cutoff_type)
  
  # ensure output directory exists
  if (!dir.exists(outdir)) {
    dir.create(outdir, recursive = TRUE)
  }
  
  # read raster
  rs <- terra::rast(rs_dir)
  
  # ---------------------------------------------------------------------------
  # 1) Fix slightly invalid lon/lat extents if needed
  # ---------------------------------------------------------------------------
  extent_fixed <- FALSE
  ext <- terra::ext(rs)
  
  if (fix_extent) {
    xmin0 <- ext[1]
    xmax0 <- ext[2]
    ymin0 <- ext[3]
    ymax0 <- ext[4]
    
    xmin1 <- xmin0
    xmax1 <- xmax0
    ymin1 <- ymin0
    ymax1 <- ymax0
    
    # Only clamp if values are just slightly out of geographic bounds
    tol <- 1  # generous tolerance, only for minor metadata/registration issues
    
    if (xmin0 < -180 && xmin0 > (-180 - tol)) xmin1 <- -180
    if (xmax0 >  180 && xmax0 < ( 180 + tol)) xmax1 <-  180
    if (ymin0 <  -90 && ymin0 > ( -90 - tol)) ymin1 <-  -90
    if (ymax0 >   90 && ymax0 < (  90 + tol)) ymax1 <-   90
    
    changed <- !isTRUE(all.equal(c(xmin0, xmax0, ymin0, ymax0),
                                 c(xmin1, xmax1, ymin1, ymax1)))
    
    if (changed) {
      terra::ext(rs) <- c(xmin1, xmax1, ymin1, ymax1)
      extent_fixed <- TRUE
      if (verbose) {
        message("Adjusted raster extent to valid lon/lat bounds.")
      }
    }
  }
  
  # ---------------------------------------------------------------------------
  # 2) Decide whether rotation is needed
  # ---------------------------------------------------------------------------
  ext <- terra::ext(rs)
  xmin <- ext[1]
  xmax <- ext[2]
  
  if (is.null(rotate_raster)) {
    # auto-detect:
    # if raster is clearly in 0-360 style longitude, rotate it
    rotate_raster <- (xmin >= 0 && xmax > 180)
  }
  
  rotated <- FALSE
  if (isTRUE(rotate_raster)) {
    rs <- terra::rotate(rs)
    rotated <- TRUE
    if (verbose) {
      message("Raster rotated from 0-360 to -180-180.")
    }
  }
  
  # ---------------------------------------------------------------------------
  # 3) Determine effective cutoff
  # ---------------------------------------------------------------------------
  vals <- terra::values(rs, mat = FALSE, na.rm = TRUE)
  vals <- vals[is.finite(vals)]
  
  if (length(vals) == 0) {
    stop("Raster has no finite values after reading.")
  }
  
  rs_min <- min(vals)
  rs_max <- max(vals)
  
  cutoff_effective <- switch(
    cutoff_type,
    normalized = rs_min + cutoff * (rs_max - rs_min),
    absolute   = cutoff
  )
  
  if (verbose) {
    message("Raster value range: [", signif(rs_min, 6), ", ", signif(rs_max, 6), "]")
    message("Cutoff type: ", cutoff_type)
    message("Input cutoff: ", cutoff)
    message("Effective cutoff used: ", signif(cutoff_effective, 6))
  }
  
  # ---------------------------------------------------------------------------
  # 4) Binary raster: 1 if > cutoff, NA otherwise
  # ---------------------------------------------------------------------------
  rs_bin <- rs > cutoff_effective
  rs_bin[rs_bin == 0] <- NA
  
  # ---------------------------------------------------------------------------
  # 5) Raster -> polygons
  # ---------------------------------------------------------------------------
  pol_terra <- terra::as.polygons(
    rs_bin,
    na.rm    = TRUE,
    dissolve = TRUE,
    values   = FALSE
  )
  
  # dissolve all into one
  pol_terra_one <- terra::aggregate(pol_terra)
  
  # ---------------------------------------------------------------------------
  # 6) Determine filename base
  # ---------------------------------------------------------------------------
  if (is.null(outfile)) {
    base <- tools::file_path_sans_ext(basename(rs_dir))
    
    cutoff_tag <- if (cutoff_type == "normalized") {
      paste0("cutnorm", gsub("\\.", "p", as.character(cutoff)))
    } else {
      paste0("cut", gsub("\\.", "p", as.character(cutoff_effective)))
    }
    
    outfile <- paste0(base, "_", cutoff_tag)
  }
  
  # RDS path
  rds_path <- file.path(outdir, paste0(outfile, ".rds"))
  
  # ---------------------------------------------------------------------------
  # 7) Convert to sf and save
  # ---------------------------------------------------------------------------
  pol_sf_one <- sf::st_as_sf(pol_terra_one)
  saveRDS(pol_sf_one, file = rds_path)
  
  if (verbose) {
    message("Saved sf RDS: ", rds_path)
  }
  
  invisible(list(
    terra            = pol_terra_one,
    sf               = pol_sf_one,
    rds_path         = rds_path,
    cutoff_input     = cutoff,
    cutoff_type      = cutoff_type,
    cutoff_effective = cutoff_effective,
    rotated          = rotated,
    extent_fixed     = extent_fixed
  ))
}

# make_front_polygon(
#   rs_dir = "outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q3_pct.tif",
#   cutoff = 0.5,
#   cutoff_type = "normalized",
#   outdir = ""
# )