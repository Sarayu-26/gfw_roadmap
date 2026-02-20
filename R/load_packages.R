# ============================
# Centralized Package Loader
# Author: Isaac Brito-Morales
# Email: ibrito@conservation.org
# ============================

# Purpose:
#   - Define and load the project package set consistently.
#   - Prefer renv when available.
#   - Avoid surprise installs on HPC unless explicitly allowed.
#
# Usage notes:
#   - Default behavior is "fail fast" if packages are missing.
#   - To allow auto-install (interactive setup only), set:
#       Sys.setenv(ALLOW_R_INSTALL = "1")
#     or from shell:
#       ALLOW_R_INSTALL=1 R -q -f R/load_packages.R

# --- Full list of required packages ---
libs <- c(
  "terra", "sf", "ggplot2", "RColorBrewer", "patchwork", "dplyr",
  "rnaturalearth", "rnaturalearthdata", "future.apply",
  "tidyr", "transformr", "stringr", "readr", "data.table",
  "doParallel", "foreach", "lwgeom", "purrr", "viridisLite", "scales",
  # helpers rely on these (installed even if not attached explicitly)
  "tibble", "units",
  # plotting device used in some figure scripts
  "ragg"
)

allow_install <- identical(Sys.getenv("ALLOW_R_INSTALL"), "1")

# --- Helper: compute missing packages ---
missing_pkgs <- function(pkgs) {
  ip <- rownames(installed.packages())
  pkgs[!(pkgs %in% ip)]
}

missing <- missing_pkgs(libs)

# --- Prefer renv if we are in a renv project ---
in_renv_project <- FALSE
if (requireNamespace("renv", quietly = TRUE)) {
  proj <- tryCatch(renv::project(), error = function(e) "")
  in_renv_project <- is.character(proj) && nzchar(proj)
}

# --- Install logic (opt-in) ---
if (length(missing)) {
  msg <- paste0(
    "Missing packages (", length(missing), "): ", paste(missing, collapse = ", ")
  )

  if (!allow_install) {
    stop(
      msg,
      "\n\nAuto-install is disabled by default (HPC-safe). ",
      "To allow installs, set ALLOW_R_INSTALL=1 and re-run.\n"
    )
  }

  if (in_renv_project) {
    message("Installing missing packages with renv: ", paste(missing, collapse = ", "))
    renv::install(missing)
    renv::snapshot(prompt = FALSE)
  } else if (requireNamespace("pak", quietly = TRUE)) {
    message("Installing missing packages with pak: ", paste(missing, collapse = ", "))
    pak::pak(missing, ask = FALSE)
  } else {
    message("Installing missing packages with install.packages(): ", paste(missing, collapse = ", "))
    install.packages(missing, dependencies = TRUE)
  }
}

# --- Load all packages (attach) ---
suppressPackageStartupMessages(
  invisible(lapply(libs, library, character.only = TRUE))
)

message("✅ All required packages loaded successfully.")