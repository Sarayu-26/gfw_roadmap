library(terra)
library(dplyr)
library(stringr)
library(maps)
library(tibble)

#code to run for-loop to create invididual rasters of species habitat suitability
#individual rasters of summed species fishing hours by gear impact
# and combined overlay of species habitat suitability masked fishing hours
#rasters saved in data 
#===============================================================================


# Read metadata
info_csv <- read.csv("data/meta_species.csv") %>% 
  as_tibble()

# Get SDM files
sdm.Rdata <- list.files(
  path = "data/SDM",
  pattern = "*SP.*\\.Rdata",
  all.files = TRUE,
  full.names = TRUE,
  recursive = FALSE
)

# Get all fishing gear rasters
gfw_rs <- list.files(
  path = "outputs/gfw_rs",
  pattern = ".rds",
  all.files = TRUE,
  full.names = TRUE,
  recursive = FALSE
)

# Function to read SDM files
read_sdm <- function(file) {
  e <- new.env()
  load(file, envir = e)
  objname <- ls(e)[1]
  obj <- e[[objname]]
  attr(obj, "species_code") <- sub(".*SP_(\\d+)\\.Rdata$", "\\1", file)
  obj
}

# Initialize a list to store all masked fishing effort rasters
masked_rasters <- list()
species_names <- c()

cat("Processing", length(sdm.Rdata), "species...\n\n")

#prepping for loop to track skipped species
skipped_species <- tibble(
  AphiaID = character(),
  CommonName = character(),
  reason = character()
)

# LOOP through each species
for(i in seq_along(sdm.Rdata)) {
  
  cat("Processing species", i, "of", length(sdm.Rdata), "\n")
  
  # Extract AphiaID from filename
  nm <- stringr::str_remove_all(basename(sdm.Rdata[i]), ".Rdata")
  nm <- unlist(stringr::str_split(nm, "_"))[5]
  
  # Get species metadata
  info_species <- info_csv %>% 
    dplyr::filter(AphiaID == nm)
  
  # Skip if species not in metadata
  if(nrow(info_species) == 0) {
    cat("  Skipping - no metadata found for AphiaID:", nm, "\n")
    skipped_species <- c(skipped_species, nm)
    next
  }
  
  # Extract species info
  common_name <- info_species$CommonName[1]
  sci_name <- info_species$ScientificName[1]
  gtt <- unique(info_species$GFWGearType)
  gtt <- unlist(stringr::str_split(gtt, "_"))
  
  cat("  Processing:", common_name, "\n")
  
  # Find matching fishing gear rasters
  files_true <- gfw_rs[basename(gfw_rs) %in% paste0("agg_cell_", gtt, ".rds")]
  
  # Skip if no matching gear files
  if(length(files_true) == 0) {
    cat("  Skipping - no matching gear files\n")
    skipped_species <- c(skipped_species, nm)
    next
  }
  
  
  # Load and sum fishing effort rasters
  rs_files_true_FF <- lapply(files_true, function(x) rast(x))
  rs_files_true_FF <- rast(rs_files_true_FF)
  rs_files_true_FF <- terra::app(rs_files_true_FF, fun = sum, na.rm = TRUE)
  
  # Load and process SDM
  tryCatch({
    sdm_data <- read_sdm(sdm.Rdata[i]) %>%
      dplyr::select(x, y, Current) %>%
      as_tibble()
    
    # Convert to raster
    rs_sdm <- vect(sdm_data, geom = c("x", "y"), crs = "EPSG:4326")
    rs_blank <- rast(res = 0.5, crs = "EPSG:4326")
    rs_sdm <- rasterize(
      rs_sdm,
      rs_blank,
      field = "Current",
      fun = "sum",
      background = NA
    )
    
    # Keep only upper quartile of habitat suitability
    qtl <- as.vector(quantile(rs_sdm[], na.rm = TRUE)[4])
    rs_sdm[] <- ifelse(rs_sdm[] >= qtl, rs_sdm[], NA)
    
    # Mask fishing effort to suitable habitat
    rs_FF <- mask(rs_files_true_FF, rs_sdm)
    
    # Save individual species rasters as .rds
    saveRDS(rs_sdm, paste0("data/rasters/habitat_", nm, ".rds"))
    saveRDS(rs_FF, paste0("data/rasters/fishing_masked_", nm, ".rds"))
    
    # Store the masked raster for combined output
    masked_rasters[[length(masked_rasters) + 1]] <- rs_FF
    species_names <- c(species_names, common_name)
    
    cat("  Successfully processed and saved:", common_name, "\n")
    
  }, error = function(e) {
    cat("  Error processing:", common_name, "-", e$message, "\n")
  })
  
}
cat("\n=== Summary of Skipped Species ===\n")
if(length(skipped_species) > 0) {
  cat("\nSkipped species:", paste(skipped_species, collapse = ", "), "\n")
} else {
  cat("\nNo species were skipped.\n")
}

cat("\n=== Combining all species habitats ===\n")

# Sum all masked rasters together
combined_raster <- rast(masked_rasters)
combined_sum <- terra::app(combined_raster, fun = sum, na.rm = TRUE)

cat("Combined", length(species_names), "species:", paste(species_names, collapse=", "), "\n\n")
# Save combined raster
saveRDS(combined_sum, "data/rasters/combined_masked_sum.rds")
#--------------------------------------------------------------------------------
#print summary of skipped species 
# Print summary
# Start with skipped_species vector
skipped_summary <- tibble(AphiaID = skipped_species) %>%
  rowwise() %>%
  mutate(
    CommonName = info_csv$CommonName[info_csv$AphiaID == AphiaID] %||% NA_character_,
    reason = ifelse(is.na(CommonName), "No metadata", "No matching gear files")
  )

# Print summary
print(skipped_summary)