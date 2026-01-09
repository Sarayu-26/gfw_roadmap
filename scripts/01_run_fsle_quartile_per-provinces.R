#!/usr/bin/env Rscript

###############################################################################
# 01_run_fsle_quartile_persistence.R
#
# Purpose:
#   - Set up the runtime environment (basic checks).
#   - Source the FSLE persistence function.
#   - Run the FSLE quartile persistence analysis with terra + future.
###############################################################################

message("[fsle] Starting FSLE quartile persistence job")

t0 <- Sys.time()

# --- 1) Optional: renv setup (DISABLED for this HPC job) --------------------
# Sys.setenv(
#   RENV_CONFIG_AUTOSNAPSHOT       = "FALSE",
#   RENV_SETTINGS_SNAPSHOT_TYPE    = "explicit"
# )
#
# if (requireNamespace("renv", quietly = TRUE)) {
#   try(renv::activate(), silent = TRUE)
# }

# --- 2) Ensure parallel helper is available ---------------------------------
if (!requireNamespace("future.apply", quietly = TRUE)) {
  stop(
    paste(
      "Package 'future.apply' is required by the FSLE persistence code.",
      "Install it with: install.packages('future.apply')"
    )
  )
}

# Explicitly load only what we need for this job
suppressPackageStartupMessages({
  library(terra)
  library(future)
  library(future.apply)
})

# --- 3) Load project code ----------------------------------------------------
# Keep these light and fast. Adjust as needed.
# if (file.exists("R/load_packages.R")) {
#   source("R/load_packages.R")
# }
if (file.exists("R/utils_helpers.R")) {
  source("R/utils_helpers.R")
}

# Main FSLE function file (your final filename)
source("R/fsle_quartiles_provinces_hpc.R")

# --- 4) Define inputs, outputs, and cores ------------------------------------
fsle_dir <- "/home/sandbox-sparc/AVISO_monthly_rs"

# Province raster aligned to FSLE grid (Longhurst IDs)
prov_raster <- "/home/sandbox-sparc/your_project_root/outputs/boundaries/longhurst_prov_id_fslegrid.tif"

outdir   <- "/scratch/sparc/fsle_quartiles_global_provinces"

# Pattern keeps only DT (delayed-time) FSLE files and excludes NRT.
pattern  <- "^dt_global_allsat_madt_fsle_.*\\.tif$"

# You control the number of cores (safe for cluster)
n_cores  <- 10

prefix   <- "fsle_quartiles_1994_2022"

message("[fsle] fsle_dir:     ", fsle_dir)
message("[fsle] prov_raster:  ", prov_raster)
message("[fsle] outdir:       ", outdir)
message("[fsle] pattern:      ", pattern)
message("[fsle] n_cores:      ", n_cores)
message("[fsle] prefix:       ", prefix)

# --- 5) Run the FSLE quartile persistence computation -----------------------
res <- compute_fsle_quartile_persistence_by_province(
  fsle_dir     = fsle_dir,
  prov_raster  = prov_raster,
  pattern      = pattern,
  outdir       = outdir,
  prefix       = prefix,
  n_cores      = n_cores,
  overwrite    = TRUE
)

# --- 6) Wrap up --------------------------------------------------------------
dt_min <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 2)
message("[fsle] Finished FSLE quartile persistence in ", dt_min, " minutes.")
message("[fsle] Files processed: ", length(res$files))
message("[fsle] Outputs written under: ", outdir)