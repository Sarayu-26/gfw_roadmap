#' Aggregate GFW Fishing Data by Cell and Gear
#'
#' This function aggregates fishing hours by grid cell
#' (`lon_bin`, `lat_bin`) and `gear`. It optionally crops the result using
#' a bounding box in lon/lat. Results are written to TSV shards on disk
#' to avoid loading the entire dataset into memory.
#'
#' @param parquet_path Path to the GFW `.parquet` dataset.
#' @param bbox_lonlat Optional bounding box in lon/lat: `c(xmin, ymin, xmax, ymax)`.
#' @param robinson (currently disabled for full-table export; project subsets later if needed).
#' @param rob_bbox Not used in this version.
#' @param save_rds Not used in this version.
#'
#' @return The path to the TSV dataset on disk.
#' @export
#'
#' @import arrow
#' @import dplyr
aggregate_gfw_by_cell <- function(
    parquet_path,
    bbox_lonlat = NULL,
    robinson = FALSE,
    rob_bbox = NULL,
    save_rds = NULL
) {
  # Open dataset lazily
  ds <- arrow::open_dataset(parquet_path, format = "parquet") %>%
    dplyr::select(lon_bin, lat_bin, gear, fishing_hours)
  
  # ---- FILTER EARLY ----
  if (!is.null(bbox_lonlat)) {
    stopifnot(length(bbox_lonlat) == 4)
    xmin <- bbox_lonlat[1]; ymin <- bbox_lonlat[2]
    xmax <- bbox_lonlat[3]; ymax <- bbox_lonlat[4]
    ds <- ds %>%
      dplyr::filter(lon_bin >= xmin, lon_bin <= xmax,
                    lat_bin >= ymin, lat_bin <= ymax)
  }
  
  # ---- AGGREGATE ----
  agg_lazy <- ds %>%
    dplyr::group_by(lon_bin, lat_bin, gear) %>%
    dplyr::summarise(
      fishing_hours_sum = sum(fishing_hours, na.rm = TRUE),
      .groups = "drop"
    )
  
  # ---- STREAM TO DISK (TSV shards) ----
  out_dir <- "outputs/agg_cell_gear_tsv"
  arrow::write_dataset(
    dataset = agg_lazy %>% dplyr::rename(lon = lon_bin, lat = lat_bin),
    path    = out_dir,
    format  = "csv",
    delimiter = "\t",
    existing_data_behavior = "overwrite"
  )
  
  return(invisible(out_dir))
}