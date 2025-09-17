#!/usr/bin/env Rscript

# --- renv guard (don’t rewrite lockfile in jobs) ---
Sys.setenv(RENV_CONFIG_AUTOSNAPSHOT = "FALSE",
           RENV_SETTINGS_SNAPSHOT_TYPE = "explicit")
if (requireNamespace("renv", quietly = TRUE)) {
  try(renv::activate(), silent = TRUE)
}

# --- load project functions ---
for (f in c("R/load_packages.R",
            "R/utils_helpers.R",
            "R/aggregate_gfw_by_cell_hpc.R")) {
  if (!file.exists(f)) stop("Missing required file: ", f)
  source(f)
}

# --- inputs / outputs ---
parquet_path <- Sys.getenv("PARQUET_PATH",
                           "data/gfw_data_by_flag_and_gear_v20250820.parquet")
bbox_ll  <- NULL
bbox_rob <- NULL
out_rds  <- "outputs/agg_cell_gear_mzc_rob.rds"

# --- thread discipline (no chunking) ---
n_threads <- suppressWarnings(as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", unset = NA)))
if (is.na(n_threads) || n_threads <= 0) {
  n_threads <- max(1L, parallel::detectCores(logical = FALSE))
}
arrow_threads <- min(n_threads, 6L)  # cap to avoid RAM blowups

Sys.setenv(ARROW_NUM_THREADS = arrow_threads)
if (requireNamespace("data.table", quietly = TRUE)) {
  data.table::setDTthreads(arrow_threads)
}

message(sprintf("[aggregate] PARQUET=%s", parquet_path))
message(sprintf("[aggregate] Using %s threads (single-pass)", arrow_threads))

# --- run aggregation with OOM retry ---
message("[aggregate] Starting…")
t0 <- Sys.time()

run <- function(thr) {
  Sys.setenv(ARROW_NUM_THREADS = thr)
  if (requireNamespace("data.table", quietly = TRUE)) {
    data.table::setDTthreads(thr)
  }
  gc()
  aggregate_gfw_by_cell(
    parquet_path = parquet_path,
    bbox_lonlat  = bbox_ll,
    robinson     = FALSE,
    rob_bbox     = bbox_rob,
    save_rds     = out_rds,
    # keep aggregation lean but single-pass
    options = list(select_cols = c("year","grid_id","hours"),
                   lazy_single_pass = TRUE,
                   verbose = TRUE)
  )
}

agg_all <- tryCatch(
  run(arrow_threads),
  error = function(e) {
    if (grepl("bad_alloc|cannot allocate|std::bad_alloc|OutOfMemory",
              conditionMessage(e), ignore.case = TRUE)) {
      message("[aggregate] OOM detected; retrying with fewer threads…")
      run(max(1L, floor(arrow_threads / 2)))
    } else {
      stop(e)
    }
  }
)

dt <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 2)
message(sprintf("[aggregate] Done in %s min → %s", dt, out_rds))