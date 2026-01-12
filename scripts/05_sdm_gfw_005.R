library(terra)
library(dplyr)
library(tidyr)
library(sf)


metadata_sps <- read.csv("data/meta_species.csv") %>% 
  as_tibble()

# Function to read SDM files
read_sdm <- function(file) {
  e <- new.env()
  load(file, envir = e)
  objname <- ls(e)[1]
  obj <- e[[objname]]
  attr(obj, "species_code") <- sub(".*SP_(\\d+)\\.Rdata$", "\\1", file)
  obj
}

# Get SDM files
sdm_aqx <- list.files(
  path = "data/aquax_sdms",
  pattern = "*SP.*\\.Rdata",
  all.files = TRUE,
  full.names = TRUE,
  recursive = FALSE
)

# Get all fishing gear txt
gfw_rs <- list.files(
  path = "data/gfw_rs",
  pattern = ".tif",
  all.files = TRUE,
  full.names = TRUE,
  recursive = FALSE
)


# LOOP through each species
for(i in seq_along(sdm_aqx)) {
  
  cat("Processing species", i, "of", length(sdm_aqx), "\n")
  
  # Extract AphiaID from filename
  nm <- stringr::str_remove_all(basename(sdm_aqx[1]), ".Rdata")
  nm <- unlist(stringr::str_split(nm, "_"))[5]
  
  # Get species metadata
  info_species <- metadata_sps %>% 
    dplyr::filter(AphiaID == nm)
  
  # Skip if species not in metadata
  if(nrow(info_species) == 0) {
    cat("  Skipping - no metadata found for AphiaID:", nm, "\n")
    skipped_species <- c(skipped_species, nm)
    next}
  
  # Extract species info
  common_name <- info_species$CommonName[1]
  sci_name <- info_species$ScientificName[1]
  gtt <- gsub("\\s+", "", unique(info_species$GFWGearType))
  gtt <- sort(unlist(stringr::str_split(gtt, "_")))
  
  cat("  Processing:", common_name, "\n")
  
  # Find matching fishing gear rasters
  files_true <- sort(gfw_rs[basename(gfw_rs) %in% paste0("agg_cell_", gtt, ".tif")])
  
  # Skip if no matching gear files
  if(length(files_true) == 0) {
    cat("  Skipping - no matching gear files\n")
    skipped_species <- c(skipped_species, nm)
    next}
  
  # Load and sum fishing effort rasters
  rs_files_true_FF <- lapply(files_true, function(x) rast(x))
  rs_files_true_FF <- rast(rs_files_true_FF)
  rs_files_true_FF <- terra::app(rs_files_true_FF, fun = sum, na.rm = TRUE)
  
  
  #
  tmpl_005 <- rast(
    ncols = 7200, nrows = 3600,
    xmin = -180, xmax = 180,
    ymin = -90,  ymax = 90,
    crs  = "EPSG:4326"
  )
  sdm_data <- read_sdm(sdm_aqx[1]) %>%
    dplyr::select(x, y, Current) %>%
    as_tibble()
  sdm_rs <- terra::rast(sdm_data, type="xyz", , crs = "EPSG:4326")
  sdm_global <- resample(sdm_rs, tmpl_005, method = "near")
  
  rs_FF <- mask(rs_files_true_FF, sdm_global)
  plot(log10(rs_FF))
  
  
  
  

  
  