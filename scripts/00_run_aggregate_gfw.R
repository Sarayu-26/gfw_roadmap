#!/usr/bin/env Rscript

# Keep renv from touching the lockfile during jobs
Sys.setenv(RENV_CONFIG_AUTOSNAPSHOT = "FALSE",
           RENV_SETTINGS_SNAPSHOT_TYPE = "explicit")
if (requireNamespace("renv", quietly = TRUE)) try(renv::activate(), silent = TRUE)

# Load your project code
source("R/load_packages.R")
source("R/utils_helpers.R")
source("R/aggregate_gfw_by_cell_hpc.R")

# Input
parquet_path <- Sys.getenv("PARQUET_PATH", "data/gfw_data_by_flag_and_gear_v20250820.parquet")

# Threads (honor ARROW_NUM_THREADS from SLURM if set)
n_threads <- suppressWarnings(as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", unset = NA)))
if (is.na(n_threads) || n_threads <= 0) {
  n_threads <- max(1L, parallel::detectCores(logical = FALSE))
}

arrow_threads <- suppressWarnings(as.integer(Sys.getenv("ARROW_NUM_THREADS", unset = n_threads)))
if (is.na(arrow_threads) || arrow_threads <= 0) {
  arrow_threads <- n_threads
}

Sys.setenv(ARROW_NUM_THREADS = arrow_threads)
if (requireNamespace("data.table", quietly = TRUE)) {
  data.table::setDTthreads(arrow_threads)
}

message(sprintf("[aggregate] PARQUET=%s", parquet_path))
message(sprintf("[aggregate] Using %s threads", arrow_threads))
message("[aggregate] Starting…")

t0 <- Sys.time()

# Call the aggregator (writes TSV shards to disk)
out_dir <- aggregate_gfw_by_cell(
  parquet_path = parquet_path,
  bbox_lonlat  = NULL,
  robinson     = FALSE,
  rob_bbox     = NULL
)

dt <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 2)
message(sprintf("[aggregate] Done in %s min → shards at %s", dt, out_dir))