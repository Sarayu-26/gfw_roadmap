# =============================================================================
# Derive ocean front hotspot polygons from raster products
#
# Author: Isaac Brito-Morales
# Email: ibrito@conservation.org
# =============================================================================


#' Create dissolved polygon mask from front raster using terra
#'
#' This function reads a front raster, applies a threshold, converts the result
#' to a binary mask, optionally rotates it from 0-360 to -180-180 when needed,
#' fixes slightly invalid lon/lat extents when present, optionally removes tiny
#' isolated patches, polygonizes it in terra, dissolves it into a single polygon,
#' and writes the result as an RDS file containing the sf object.
#'
#' Thresholding can be done in three ways:
#'   - absolute: use cutoff directly
#'   - normalized: interpret cutoff relative to raster range
#'   - quantile: interpret cutoff as an upper-tail percentile (e.g. 0.9 = top decile)
#'
#' By default, cutoff is interpreted in absolute units so existing FSLE workflows
#' can remain unchanged if desired.
#'
#' @param rs_dir Character. Path to raster file.
#' @param cutoff Numeric. Threshold value. Default = 50.
#' @param cutoff_type Character. One of "absolute", "normalized", or "quantile".
#' @param outdir Character. Directory where the output polygon file will be stored.
#' @param outfile Optional character. Name of output file. If not provided, a name
#'   will be auto-generated based on the input raster name and threshold.
#' @param rotate_raster Logical or NULL. If TRUE/FALSE, force behavior. If NULL,
#'   function decides automatically from raster extent. Default = NULL.
#' @param fix_extent Logical. If TRUE, clamps slightly invalid lon/lat extents
#'   to valid bounds when needed. Default = TRUE.
#' @param remove_small_patches Logical. If TRUE, removes connected patches smaller
#'   than min_patch_cells after thresholding. Default = FALSE.
#' @param min_patch_cells Integer. Minimum number of cells required to retain a patch.
#'   Used only if remove_small_patches = TRUE. Default = 20.
#' @param patch_directions Integer. Directions passed to terra::patches(). Default = 8.
#' @param quantile_ignore_zero Logical. If TRUE and cutoff_type = "quantile",
#'   compute the quantile using only values > 0. Default = TRUE.
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
#'   - small_patches_removed: whether patch filtering was applied
#'
#' @export
make_front_polygon <- function(rs_dir,
                               cutoff = 50,
                               cutoff_type = c("absolute", "normalized", "quantile"),
                               outdir,
                               outfile = NULL,
                               rotate_raster = NULL,
                               fix_extent = TRUE,
                               remove_small_patches = FALSE,
                               min_patch_cells = 20,
                               patch_directions = 8,
                               quantile_ignore_zero = TRUE,
                               verbose = TRUE) {
  
  cutoff_type <- match.arg(cutoff_type)
  
  if (!dir.exists(outdir)) {
    dir.create(outdir, recursive = TRUE)
  }
  
  rs <- terra::rast(rs_dir)
  
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
    
    tol <- 1
    
    if (xmin0 < -180 && xmin0 > (-180 - tol)) xmin1 <- -180
    if (xmax0 >  180 && xmax0 < ( 180 + tol)) xmax1 <-  180
    if (ymin0 <  -90 && ymin0 > ( -90 - tol)) ymin1 <-  -90
    if (ymax0 >   90 && ymax0 < (  90 + tol)) ymax1 <-   90
    
    changed <- !isTRUE(all.equal(c(xmin0, xmax0, ymin0, ymax0),
                                 c(xmin1, xmax1, ymin1, ymax1)))
    
    if (changed) {
      terra::ext(rs) <- c(xmin1, xmax1, ymin1, ymax1)
      extent_fixed <- TRUE
      if (verbose) message("Adjusted raster extent to valid lon/lat bounds.")
    }
  }
  
  ext <- terra::ext(rs)
  xmin <- ext[1]
  xmax <- ext[2]
  
  if (is.null(rotate_raster)) {
    rotate_raster <- (xmin >= 0 && xmax > 180)
  }
  
  rotated <- FALSE
  if (isTRUE(rotate_raster)) {
    rs <- terra::rotate(rs)
    rotated <- TRUE
    if (verbose) message("Raster rotated from 0-360 to -180-180.")
  }
  
  vals <- terra::values(rs, mat = FALSE, na.rm = TRUE)
  vals <- vals[is.finite(vals)]
  
  if (length(vals) == 0) {
    stop("Raster has no finite values after reading.")
  }
  
  rs_min <- min(vals)
  rs_max <- max(vals)
  
  vals_q <- vals
  if (cutoff_type == "quantile" && quantile_ignore_zero) {
    vals_q <- vals[vals > 0]
    if (length(vals_q) == 0) {
      stop("No positive finite values available for quantile thresholding.")
    }
  }
  
  cutoff_effective <- switch(
    cutoff_type,
    absolute   = cutoff,
    normalized = rs_min + cutoff * (rs_max - rs_min),
    quantile   = as.numeric(stats::quantile(vals_q, probs = cutoff, na.rm = TRUE))
  )
  
  if (verbose) {
    message("Raster value range: [", signif(rs_min, 6), ", ", signif(rs_max, 6), "]")
    message("Cutoff type: ", cutoff_type)
    message("Input cutoff: ", cutoff)
    message("Effective cutoff used: ", signif(cutoff_effective, 6))
  }
  
  rs_bin <- terra::ifel(rs > cutoff_effective, 1, NA)
  
  small_patches_removed <- FALSE
  
  if (remove_small_patches) {
    cl <- terra::patches(rs_bin, directions = patch_directions)
    fr <- terra::freq(cl)
    
    if (!is.null(fr) && nrow(fr) > 0) {
      keep_ids <- fr$value[fr$count >= min_patch_cells]
      rs_bin <- terra::ifel(cl %in% keep_ids, 1, NA)
      small_patches_removed <- TRUE
      
      if (verbose) {
        message("Removed patches smaller than ", min_patch_cells, " cells.")
      }
    }
  }
  
  pol_terra <- terra::as.polygons(
    rs_bin,
    na.rm    = TRUE,
    dissolve = TRUE,
    values   = FALSE
  )
  
  pol_terra_one <- terra::aggregate(pol_terra)
  
  if (is.null(outfile)) {
    base <- tools::file_path_sans_ext(basename(rs_dir))
    
    cutoff_tag <- switch(
      cutoff_type,
      absolute   = paste0("cut", gsub("\\.", "p", as.character(cutoff_effective))),
      normalized = paste0("cutnorm", gsub("\\.", "p", as.character(cutoff))),
      quantile   = paste0("cutq", gsub("\\.", "p", as.character(cutoff)))
    )
    
    patch_tag <- if (remove_small_patches) paste0("_minpatch", min_patch_cells) else ""
    
    outfile <- paste0(base, "_", cutoff_tag, patch_tag)
  }
  
  rds_path <- file.path(outdir, paste0(outfile, ".rds"))
  
  pol_sf_one <- sf::st_as_sf(pol_terra_one)
  saveRDS(pol_sf_one, file = rds_path)
  
  if (verbose) message("Saved sf RDS: ", rds_path)
  
  invisible(list(
    terra                = pol_terra_one,
    sf                   = pol_sf_one,
    rds_path             = rds_path,
    cutoff_input         = cutoff,
    cutoff_type          = cutoff_type,
    cutoff_effective     = cutoff_effective,
    rotated              = rotated,
    extent_fixed         = extent_fixed,
    small_patches_removed = small_patches_removed
  ))
}