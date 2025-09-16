# ============================
# Centralized Package Loader
# Author: Isaac Brito-Morales
# Email: ibrito@conservation.org
# ============================

# --- Full list of required packages ---
libs <- c(
  "terra", "sf", "ggplot2", "RColorBrewer", "patchwork", "dplyr",
  "rnaturalearth", "rnaturalearthdata", "future.apply",
  "tidyr", "transformr", "stringr", "readr", "data.table",
  "doParallel", "foreach", "lwgeom", "purrr", "viridisLite", "scales"
)

# --- Prefer renv if active ---
if (requireNamespace("renv", quietly = TRUE) && renv::project() != "") {
  # Check what's missing
  missing <- libs[!(libs %in% rownames(installed.packages()))]
  if (length(missing)) {
    message("Installing missing packages with renv: ", paste(missing, collapse = ", "))
    renv::install(missing)
    renv::snapshot()  # Keep lockfile up to date
  }
} else {
  # --- Fallback to pak if available (faster) ---
  if (requireNamespace("pak", quietly = TRUE)) {
    missing <- libs[!(libs %in% rownames(installed.packages()))]
    if (length(missing)) {
      message("Installing missing packages with pak: ", paste(missing, collapse = ", "))
      pak::pak(missing, ask = FALSE)
    }
  } else {
    # --- Base R install as last resort ---
    missing <- libs[!(libs %in% rownames(installed.packages()))]
    if (length(missing)) {
      message("Installing missing packages with install.packages(): ", paste(missing, collapse = ", "))
      install.packages(missing, dependencies = TRUE)
    }
  }
}

# --- Load all packages ---
invisible(lapply(libs, library, character.only = TRUE))

message("✅ All required packages loaded successfully.")
