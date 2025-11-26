#' Create dissolved polygon mask from FSLE (or any raster) using terra
#'
#' This function takes an input raster file, thresholds it using a cutoff value,
#' converts it to a binary mask, optionally rotates it from 0–360 to -180–180 degrees,
#' polygonizes it in terra, dissolves it into a single polygon, and writes the result
#' as an RDS file containing the sf object.
#'
#' @param rs_dir Character. Path to raster file, e.g. 
#'   "outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q3_pct.tif"
#' @param cutoff Numeric. Threshold value for raster filtering. Default = 50.
#' @param outdir Character. Directory where the output polygon file will be stored.
#' @param outfile Optional character. Name of output file. If not provided, a name will
#'   be auto-generated based on the input raster name and cutoff.
#' @param rotate_raster Logical. If TRUE, applies terra::rotate() to shift 
#'   raster from 0–360 to -180–180. Default TRUE.
#'
#' @return A list containing:
#'   - terra: the dissolved terra polygon object
#'   - sf: the dissolved polygon as an sf object
#'   - rds_path: the RDS output file path
#'
#' @export
make_front_polygon <- function(rs_dir,
                               cutoff = 50,
                               outdir,
                               outfile = NULL,
                               rotate_raster = TRUE) {
  
  # ensure output directory exists
  if (!dir.exists(outdir)) {
    dir.create(outdir, recursive = TRUE)
  }
  
  # read raster
  rs <- terra::rast(rs_dir)
  
  # binary raster: 1 if > cutoff, NA otherwise
  rs_bin <- rs > cutoff
  rs_bin[rs_bin == 0] <- NA
  
  # optional rotate (0–360 to -180–180)
  if (rotate_raster) {
    rs_bin <- terra::rotate(rs_bin)
  }
  
  # raster → polygons
  pol_terra <- terra::as.polygons(
    rs_bin,
    na.rm   = TRUE,
    dissolve = TRUE,
    values   = FALSE
  )
  
  # dissolve all into one
  pol_terra_one <- terra::aggregate(pol_terra)
  
  # determine filename base
  if (is.null(outfile)) {
    base <- tools::file_path_sans_ext(basename(rs_dir))
    outfile <- paste0(base, "_cut", cutoff)
  }
  
  # RDS path
  rds_path <- file.path(outdir, paste0(outfile, ".rds"))
  
  # convert to sf and save
  pol_sf_one <- sf::st_as_sf(pol_terra_one)
  saveRDS(pol_sf_one, file = rds_path)
  message("Saved sf RDS: ", rds_path)
  
  invisible(list(
    terra     = pol_terra_one,
    sf        = pol_sf_one,
    rds_path  = rds_path
  ))
}