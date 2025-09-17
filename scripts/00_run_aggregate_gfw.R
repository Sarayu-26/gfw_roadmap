#!/usr/bin/env Rscript

###############################################################################
# 00_run_aggregate_gfw.R
#
# Purpose:
#   - Prepare the runtime environment (renv, threads).
#   - Call the per-gear, out-of-core aggregator.
#   - Explain clearly how CPU/threads interact and how to switch bbox scope.
#
# Key ideas:
#   - Parallelism happens at TWO levels:
#       (1) Number of R workers (processes) handling different gears in parallel.
#           Controlled by env var GEAR_WORKERS (or SLURM_CPUS_PER_TASK).
#       (2) Number of Arrow threads *inside each worker* for scan/aggregate.
#           Controlled by env var ARROW_NUM_THREADS.
#     Total CPU demand ≈ GEAR_WORKERS × ARROW_NUM_THREADS (plus minor overhead).
#
#   - Memory safety:
#       * Aggregation is per-gear and written to disk (TSV shards), avoiding
#         collecting a giant table into R memory.
#       * Keep ARROW_NUM_THREADS small (e.g., 1) to reduce peak RAM per worker.
###############################################################################

# --- 1) Prevent renv from touching the lockfile in batch jobs ----------------
Sys.setenv(
  RENV_CONFIG_AUTOSNAPSHOT = "FALSE",      # don't auto-snapshot deps
  RENV_SETTINGS_SNAPSHOT_TYPE = "explicit" # snapshot only when you ask
)
if (requireNamespace("renv", quietly = TRUE)) {
  # Activate the project library; errors are non-fatal if renv isn't present
  try(renv::activate(), silent = TRUE)
}

# --- 2) Ensure parallel helper is available ---------------------------------
# The aggregator uses future.apply to parallelize over gears.
if (!requireNamespace("future.apply", quietly = TRUE)) {
  stop(
    paste(
      "Package 'future.apply' is required by the aggregator.",
      "Install it in this renv with: renv::install('future.apply')"
    )
  )
}

# --- 3) Load project code ----------------------------------------------------
# Keep these small and fast (no heavy work during sourcing).
source("R/load_packages.R")          # loads CRAN pkgs, sets options, etc.
source("R/utils_helpers.R")          # any small helpers you wrote
source("R/aggregate_gfw_by_cell_hpc.R")  # the main per-gear aggregator

# --- 4) Input data location --------------------------------------------------
# PARQUET_PATH can be passed from SLURM (recommended). If not set, default.
parquet_path <- Sys.getenv(
  "PARQUET_PATH",
  unset = "data/gfw_data_by_flag_and_gear_v20250820.parquet"
)

# --- 5) Threads: Arrow (per worker) and data.table (if used) ----------------
# SLURM_CPUS_PER_TASK is how many CPUs the job was granted (total).
# We'll *also* read ARROW_NUM_THREADS (how many threads each worker uses).
# Note: the aggregator itself controls the number of workers (GEAR_WORKERS).
n_threads <- suppressWarnings(
  as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", unset = NA))
)
if (is.na(n_threads) || n_threads <= 0) {
  # Fallback to physical cores if SLURM var isn't present (e.g., running locally)
  n_threads <- max(1L, parallel::detectCores(logical = FALSE))
}

# Arrow threads per worker. If ARROW_NUM_THREADS isn't provided, we default
# to the total SLURM CPUs; but in cluster practice, set ARROW_NUM_THREADS=1
# in the SLURM script to minimize per-worker RAM and avoid oversubscription.
arrow_threads <- suppressWarnings(
  as.integer(Sys.getenv("ARROW_NUM_THREADS", unset = n_threads))
)
if (is.na(arrow_threads) || arrow_threads <= 0) arrow_threads <- n_threads

# Apply Arrow and data.table thread limits for THIS launcher process.
# (The aggregator will also set Arrow threads inside each worker.)
Sys.setenv(ARROW_NUM_THREADS = arrow_threads)
if (requireNamespace("data.table", quietly = TRUE)) {
  data.table::setDTthreads(arrow_threads)
}

# --- 6) Concurrency information for logs ------------------------------------
# GEAR_WORKERS controls number of R workers in the aggregator.
# If not set, the function will fall back to SLURM_CPUS_PER_TASK or 2.
gear_workers <- Sys.getenv(
  "GEAR_WORKERS",
  unset = Sys.getenv("SLURM_CPUS_PER_TASK", unset = "unset")
)

message(sprintf("[aggregate] PARQUET = %s", parquet_path))
message(sprintf("[aggregate] Arrow threads per worker = %s", arrow_threads))
message(sprintf("[aggregate] R workers (GEAR_WORKERS) = %s", gear_workers))
message("[aggregate] Starting…")

t0 <- Sys.time()

# --- 7) Spatial scope toggle -------------------------------------------------
# Choose ONE of the following:
#   - For full domain, set bbox_lonlat <- NULL
#   - For a smaller test region, set bbox_lonlat to c(xmin, ymin, xmax, ymax)
#
# Tip: Start with a bbox to validate the pipeline, then switch to NULL.
# Example test region (Mozambique Channel-ish):
bbox_lonlat <- c(30, -35, 65,  0)
# bbox_lonlat <- NULL  # <-- set to NULL for full extent; change to a vector to test

# --- 8) Run the aggregator ---------------------------------------------------
# This function will:
#   * inspect available gears,
#   * spin up up to GEAR_WORKERS workers (or fallback),
#   * for each gear: filter + group + summarise,
#   * stream results to disk (tab-delimited shards) under:
#       outputs/agg_cell_<gear>_tsv/
# No giant collect() happens, so memory stays bounded.
out_dir <- aggregate_gfw_by_cell(
  parquet_path = parquet_path,
  bbox_lonlat  = bbox_lonlat,
  robinson     = FALSE,  # Projection is intentionally disabled for full-table export
  rob_bbox     = NULL
)

# --- 9) Wrap up --------------------------------------------------------------
dt <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 2)
message(sprintf("[aggregate] Done in %s min → per-gear shards under %s", dt, out_dir))

# FYI:
# - If you want a *single file per gear*, your SLURM script can merge each
#   gear's TSV shards into outputs/agg_cell_<gear>_full.txt using awk:
#     awk 'FNR==1 && NR!=1 {next} {print}' "$dir"/*.csv > "outputs/agg_cell_${gear}_full.txt"
#
# - If you later need an .rds or to project to Robinson, read back a subset
#   (region of interest) from the per-gear TSVs or Parquet and only then collect.
