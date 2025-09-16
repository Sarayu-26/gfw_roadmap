#!/usr/bin/env Rscript

# Disable renv autosnapshot (prevents SLURM from rewriting renv.lock)
Sys.setenv(RENV_CONFIG_AUTOSNAPSHOT = "FALSE")
Sys.setenv(RENV_SETTINGS_SNAPSHOT_TYPE = "explicit")

# Activate renv if present (non-fatal)
if (requireNamespace("renv", quietly = TRUE)) {
  try(renv::activate(), silent = TRUE)
}

# Source your function
source("R/load_packages.R")
source("R/utils_helpers.R")
source("R/aggregate_gfw_by_cell_hpc.R")

# Params (WORLD)
parquet_path <- Sys.getenv("PARQUET_PATH", "data/gfw_data_by_flag_and_gear_v20250820.parquet")
bbox_ll  <- NULL
bbox_rob <- NULL
out_rds  <- "outputs/agg_cell_gear_mzc_rob.rds"

# Threads: respect SLURM; fallback to local cores
n_threads <- as.numeric(Sys.getenv("SLURM_CPUS_PER_TASK", unset = NA))
if (is.na(n_threads)) n_threads <- parallel::detectCores()
Sys.setenv(ARROW_NUM_THREADS = n_threads)
message(sprintf("[aggregate] Using %s threads", n_threads))

# Run
message("[aggregate] Starting…")
t0 <- Sys.time()
agg_all <- aggregate_gfw_by_cell(
  parquet_path = parquet_path,
  bbox_lonlat  = bbox_ll,
  robinson     = FALSE,
  rob_bbox     = bbox_rob,
  save_rds     = out_rds
)
dt <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 2)
message(sprintf("[aggregate] Done in %s min → %s", dt, out_rds))