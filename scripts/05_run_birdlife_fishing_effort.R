#!/usr/bin/env Rscript

###############################################################################
# 05_run_birdlife_gfw_fishing_effort.R
#
# Purpose:
#   - Set up the runtime environment (basic checks).
#   - Source the BirdLife + GFW fishing effort function (build_rs_FF_birdlife).
#   - Run the BirdLife seabird rasters + GFW fishing effort masking workflow
#     with terra + future.
###############################################################################

message("[rs_FF] Starting BirdLife seabirds + GFW fishing effort job")

t0 <- Sys.time()

# --- 1) Define project home + move there ------------------------------------
proj_home <- "/home/sandbox-sparc/gfw_roadmap"
if (!dir.exists(proj_home)) stop("[rs_FF] Project home not found: ", proj_home)

setwd(proj_home)
message("[rs_FF] Working directory set to: ", getwd())

# --- 2) Optional: renv setup (DISABLED for this HPC job) --------------------
# Sys.setenv(
#   RENV_CONFIG_AUTOSNAPSHOT       = "FALSE",
#   RENV_SETTINGS_SNAPSHOT_TYPE    = "explicit"
# )
#
# if (requireNamespace("renv", quietly = TRUE)) {
#   try(renv::activate(), silent = TRUE)
# }

# --- 3) Ensure parallel helper is available ---------------------------------
if (!requireNamespace("future.apply", quietly = TRUE)) {
  stop(
    paste(
      "Package 'future.apply' is required by the BirdLife + GFW fishing effort code.",
      "Install it with: install.packages('future.apply')"
    )
  )
}

# Explicitly load only what we need for this job
suppressPackageStartupMessages({
  library(terra)
  library(future)
  library(future.apply)
  library(dplyr)
  library(tidyr)
  library(sf)
})

# --- 4) Load project code ----------------------------------------------------
# NOTE: keep this file in /R/ next to the SDM version.
source("R/birdlife_gfw_fishing_effort.R")

# --- 5) Define inputs, outputs, and cores ------------------------------------
bird_dir     <- file.path(proj_home, "data/birdlife_rs_005")
metadata_csv <- file.path(proj_home, "data/seabird_gfw_roadmap.csv")
gfw_dir      <- file.path(proj_home, "data/gfw_rs")
outdir       <- "/home/sandbox-sparc/rs_FF_birdlife"

bird_pattern <- ".*_005\\.tif$"
gfw_pattern  <- "\\.tif$"

n_cores      <- 8

# Ensure output directory exists
if (!dir.exists(outdir)) {
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
}

message("[rs_FF] bird_dir:     ", bird_dir)
message("[rs_FF] metadata_csv:", metadata_csv)
message("[rs_FF] gfw_dir:     ", gfw_dir)
message("[rs_FF] outdir:      ", outdir)
message("[rs_FF] bird_pattern:", bird_pattern)
message("[rs_FF] gfw_pattern: ", gfw_pattern)
message("[rs_FF] n_cores:     ", n_cores)

# --- 6) Run the workflow -----------------------------------------------------
res <- build_rs_FF_birdlife(
  bird_dir      = bird_dir,
  bird_pattern  = bird_pattern,
  metadata_csv  = metadata_csv,
  gfw_dir       = gfw_dir,
  gfw_pattern   = gfw_pattern,
  outdir        = outdir,
  n_cores       = n_cores,
  overwrite     = TRUE,
  do_plot       = FALSE,
  check_grid    = TRUE
)

# --- 7) Wrap up --------------------------------------------------------------
dt_min <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 2)

n_files   <- length(res$bird_files)
n_skipped <- length(res$skipped)

message("[rs_FF] Finished BirdLife + GFW fishing effort in ", dt_min, " minutes.")
message("[rs_FF] BirdLife rasters processed: ", n_files)
message("[rs_FF] Species skipped:            ", n_skipped)
if (n_skipped > 0) {
  message("[rs_FF] Skipped Scientific Names (first 25): ",
          paste(head(res$skipped, 25), collapse = ", "))
}
message("[rs_FF] Outputs written under: ", outdir)