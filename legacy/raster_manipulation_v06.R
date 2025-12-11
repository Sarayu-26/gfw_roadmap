library(terra)
library(dplyr)
library(stringr)
library(maps)

#This for-loop works!! It creates *individual* pdf maps for each species
#====================================================================================

# Read metadata
info_csv <- read.csv("data/meta_species.csv") %>% 
  as_tibble()

# Get SDM files
sdm.Rdata <- list.files(
  path = "data",
  pattern = "*SP.*.Rdata",
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
    next
  }
  
  # Load and sum fishing effort rasters
  rs_files_true_FF <- lapply(files_true, function(x) rast(x))
  rs_files_true_FF <- rast(rs_files_true_FF)
  rs_files_true_FF <- terra::app(rs_files_true_FF, fun = sum, na.rm = TRUE)
  
  # Rotate fishing rasters to match Pacific-centered view (0-360)
  rs_files_true_FF <- rotate(rs_files_true_FF)
  
  # Load and process SDM
  tryCatch({
    sdm_data <- read_sdm(sdm.Rdata[i]) %>%
      dplyr::select(x, y, Current) %>%
      as_tibble()
    
    # Convert to raster - Pacific-centered
    # Shift longitude from -180:180 to 0:360
    sdm_data$x <- ifelse(sdm_data$x < 0, sdm_data$x + 360, sdm_data$x)
    
    rs_sdm <- vect(sdm_data, geom = c("x", "y"), crs = "EPSG:4326")
    rs_blank <- rast(res = 0.5, crs = "EPSG:4326", ext = ext(0, 360, -90, 90))
    
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
    
    # Store the masked raster
    masked_rasters[[length(masked_rasters) + 1]] <- rs_FF
    species_names <- c(species_names, common_name)
    
    cat("  Successfully processed:", common_name, "\n")
    
  }, error = function(e) {
    cat("  Error processing:", common_name, "-", e$message, "\n")
  })
  
}

cat("\n=== Combining all species habitats ===\n")

# Sum all masked rasters together
combined_raster <- rast(masked_rasters)
combined_sum <- terra::app(combined_raster, fun = sum, na.rm = TRUE)

cat("Combined", length(species_names), "species:", paste(species_names, collapse=", "), "\n\n")

# Create the combined plot
pdf("outputs/3_species_overlay.pdf", width = 30, height = 16)

plot(
  log(combined_sum),
  main = "Combined Fishing Effort Across Three Species Habitat Suitability Ranges",
  legend = TRUE,
  plg = list(title = "Log Fishing Hours"),
  cex.main = 2
)

# Shifting world map to Pacific-centered view
world_map <- map("world", plot = FALSE, wrap = c(0, 360))
world_map$x <- ifelse(world_map$x < 0, world_map$x + 360, world_map$x)
map(world_map, add = TRUE)

mtext(
  paste0(
    "Figure: Summed log-scale fishing hours across the upper quartile habitat suitability zones for ",
    length(species_names), " species: ", paste(species_names, collapse=", "), 
    ". The map shows total fishing pressure by sum of hours across gears to log scale."
  ),
  side = 1,
  line = 4,
  cex = 1.1
)

dev.off()
cat("\n✓ Combined overlay map saved to: outputs/3species_overlay.pdf\n")