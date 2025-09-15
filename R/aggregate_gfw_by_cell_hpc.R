#' Aggregate GFW Fishing Data by Cell and Gear
#'
#' This function aggregates fishing hours and fishing kW-hours by grid cell
#' (`lon_bin`, `lat_bin`) and `gear`. It optionally crops the result using
#' a bounding box in lon/lat, projects to Robinson, and/or crops in Robinson.
#' Results can be saved as `.rds`.
#'
#' @param parquet_path Path to the GFW `.parquet` dataset.
#' @param bbox_lonlat Optional bounding box in lon/lat: `c(xmin, ymin, xmax, ymax)`.
#' @param robinson Logical; if `TRUE`, returns an `sf` object projected to Robinson.
#' @param rob_bbox Optional bounding box in Robinson meters: `c(xmin, ymin, xmax, ymax)`.
#' @param save_rds Optional file path to save the result as an `.rds`.
#'
#' @return A tibble (if `robinson = FALSE`) or an `sf` object (if `robinson = TRUE`).
#' @export
#'
#' @import arrow
#' @import dplyr
#' @importFrom sf st_as_sf st_transform st_crs st_bbox st_crop
aggregate_gfw_by_cell <- function(
    parquet_path,
    bbox_lonlat = NULL,
    robinson = FALSE,
    rob_bbox = NULL,
    save_rds = NULL
) {
  # Open dataset lazily (no columns= here; use select() next)
  ds <- arrow::open_dataset(parquet_path, format = "parquet") %>%
    dplyr::select(lon_bin, lat_bin, gear, fishing_hours, fishing_kw_hours)
  
  # ---- FILTER EARLY (before group_by/summarise) ----
  if (!is.null(bbox_lonlat)) {
    stopifnot(length(bbox_lonlat) == 4)
    xmin <- bbox_lonlat[1]; ymin <- bbox_lonlat[2]
    xmax <- bbox_lonlat[3]; ymax <- bbox_lonlat[4]
    ds <- ds %>%
      dplyr::filter(lon_bin >= xmin, lon_bin <= xmax,
                    lat_bin >= ymin, lat_bin <= ymax)
  }
  
  # Aggregate by lon/lat cell + gear (lazy, efficient)
  agg_lazy <- ds %>%
    dplyr::group_by(lon_bin, lat_bin, gear) %>%
    dplyr::summarise(
      fishing_hours_sum    = sum(fishing_hours, na.rm = TRUE),
      fishing_kw_hours_sum = sum(fishing_kw_hours, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Collect aggregated dataset into memory
  agg_df <- agg_lazy %>%
    dplyr::collect() %>%
    dplyr::rename(lon = lon_bin, lat = lat_bin)
  
  # Optional projection to Robinson and cropping in Robinson coordinates
  if (robinson) {
    agg_sf <- sf::st_as_sf(agg_df, coords = c("lon", "lat"), crs = 4326)
    rob_crs <- tryCatch(sf::st_crs("ESRI:54030"), error = function(e) NULL)
    if (is.null(rob_crs)) {
      rob_crs <- sf::st_crs("+proj=robin +datum=WGS84 +units=m +no_defs")
    }
    agg_sf_rob <- sf::st_transform(agg_sf, crs = rob_crs)
    
    if (!is.null(rob_bbox)) {
      stopifnot(length(rob_bbox) == 4)
      rb <- sf::st_bbox(
        c(xmin = rob_bbox[1], ymin = rob_bbox[2],
          xmax = rob_bbox[3], ymax = rob_bbox[4]),
        crs = rob_crs
      )
      agg_sf_rob <- sf::st_crop(agg_sf_rob, rb)
    }
    
    if (!is.null(save_rds)) saveRDS(agg_sf_rob, save_rds)
    return(agg_sf_rob)
  } else {
    if (!is.null(save_rds)) saveRDS(agg_df, save_rds)
    return(agg_df)
  }
}