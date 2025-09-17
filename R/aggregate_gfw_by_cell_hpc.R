#' Aggregate GFW Fishing Data by Cell and Gear (parallel per-gear, out-of-core)
#'
#' Aggregates fishing hours by grid cell (`lon_bin`, `lat_bin`) and `gear`,
#' optionally within a lon/lat bounding box. Results are written **per gear**
#' to tab-delimited shards on disk to avoid loading the full table into memory.
#' One directory is created per gear: `outputs/agg_cell_<gear>_tsv/`.
#'
#' Internally, it first identifies the distinct gear types and then runs a
#' per-gear aggregation in parallel. The number of workers defaults to the
#' environment variable `GEAR_WORKERS` (if set), else `SLURM_CPUS_PER_TASK`,
#' else `2`. To avoid oversubscription, set `ARROW_NUM_THREADS=1` externally.
#'
#' @param parquet_path Path to the GFW `.parquet` dataset.
#' @param bbox_lonlat Optional bounding box in lon/lat: `c(xmin, ymin, xmax, ymax)`.
#' @param robinson (disabled for full-table export; project subsets later if needed).
#' @param rob_bbox Not used in this version.
#' @param save_rds Not used in this version.
#'
#' @return Invisibly returns the base output directory (`"outputs"`).
#' @export
#'
#' @import arrow
#' @import dplyr
#' @importFrom future plan supportsMulticore
#' @importFrom future.apply future_lapply
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
  
  # ---- FILTER EARLY (optional bbox) ----
  if (!is.null(bbox_lonlat)) {
    stopifnot(length(bbox_lonlat) == 4)
    xmin <- bbox_lonlat[1]; ymin <- bbox_lonlat[2]
    xmax <- bbox_lonlat[3]; ymax <- bbox_lonlat[4]
    ds <- ds %>%
      dplyr::filter(lon_bin >= xmin, lon_bin <= xmax,
                    lat_bin >= ymin, lat_bin <= ymax)
  }
  
  # ---- GEARS (tiny collect just for names) ----
  gears <- ds %>%
    dplyr::select(gear) %>%
    dplyr::filter(!is.na(gear), gear != "") %>%
    dplyr::distinct() %>%
    dplyr::arrange(gear) %>%
    dplyr::collect() %>%
    dplyr::pull(gear)
  
  if (length(gears) == 0L) {
    warning("No non-missing gear values found; nothing to aggregate.")
    return(invisible("outputs"))
  }
  
  # Helper to make safe names for files/dirs
  sanitize <- function(x) gsub("[^A-Za-z0-9._-]+", "_", x)
  
  # ---- PARALLEL PLAN (per gear) ----
  workers_env <- suppressWarnings(as.integer(
    Sys.getenv("GEAR_WORKERS",
               unset = Sys.getenv("SLURM_CPUS_PER_TASK", unset = "2")
    )
  ))
  if (is.na(workers_env) || workers_env < 1L) workers_env <- 2L
  workers <- max(1L, min(length(gears), workers_env))
  
  if (future::supportsMulticore()) {
    future::plan(future::multicore, workers = workers)
  } else {
    future::plan(future::multisession, workers = workers)
  }
  on.exit(future::plan(future::sequential), add = TRUE)
  
  base_out <- "outputs"
  
  # ---- AGGREGATE & WRITE PER GEAR (parallel, no collect) ----
  future.apply::future_lapply(gears, function(g) {
    g_safe    <- sanitize(g)
    g_tsv_dir <- file.path(base_out, paste0("agg_cell_", g_safe, "_tsv"))
    dir.create(g_tsv_dir, recursive = TRUE, showWarnings = FALSE)
    
    # Keep Arrow lean inside each worker; override to 1 if unset/invalid
    at <- suppressWarnings(as.integer(Sys.getenv("ARROW_NUM_THREADS", unset = "1")))
    if (is.na(at) || at < 1L) at <- 1L
    Sys.setenv(ARROW_NUM_THREADS = at)
    
    part <- ds %>%
      dplyr::filter(gear == g) %>%
      dplyr::group_by(lon_bin, lat_bin, gear) %>%
      dplyr::summarise(
        fishing_hours_sum = sum(fishing_hours, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::rename(lon = lon_bin, lat = lat_bin)
    
    arrow::write_dataset(
      dataset = part,
      path    = g_tsv_dir,
      format  = "csv",
      delimiter = "\t",
      existing_data_behavior = "overwrite"
    )
    
    invisible(g_tsv_dir)
  }, future.seed = TRUE)
  
  return(invisible(base_out))
}