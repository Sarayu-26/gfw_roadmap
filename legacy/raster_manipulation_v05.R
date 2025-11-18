library(terra)
library(dplyr)
library(stringr)
library(maps)

#This for loop works!! It creates individual pdf maps for each species
#====================================================================================
# Set  paths
sdm_path <- "data"  # CHANGE to Dropbox SDM folder path
output_path <- "outputs"

# Read metadata
info_csv <- read.csv("data/meta_species.csv") %>% 
  as_tibble()

# Get all SDM files
sdm.Rdata <- list.files(
  path = sdm_path,
  pattern = "*SP.*.Rdata",
  all.files = TRUE,
  full.names = TRUE,
  recursive = FALSE
)

# Get all fishing gear rasters (only need to list once)
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

# MAIN LOOP
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
  
  # Extract species names and gears
  common_name <- info_species$CommonName[1]
  sci_name <- info_species$ScientificName[1]
  gtt <- unique(info_species$GFWGearType)
  gtt <- unlist(stringr::str_split(gtt, "_"))
  
  # Format gear list for caption
  gear_list <- paste(gtt, collapse = ", ")
  
  # Find matching fishing gear rasters
  files_true <- gfw_rs[basename(gfw_rs) %in% paste0("agg_cell_", gtt, ".rds")]
  
  # Skip if no matching gear files
  if(length(files_true) == 0) {
    cat("  Skipping - no matching gear files for:", common_name, "\n")
    next
  }
  
  # Load and sum fishing effort rasters
  rs_files_true_FF <- lapply(files_true, function(x) rast(x))
  rs_files_true_FF <- rast(rs_files_true_FF)
  rs_files_true_FF <- terra::app(rs_files_true_FF, fun = sum, na.rm = TRUE)
  
  # Load and process SDM th
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
    
    # Create PDF
    pdf_filename <- paste0(output_path, "/SP_", nm, ".pdf")
    pdf(pdf_filename, width = 30, height = 16)
    
    plot(
      log(rs_FF),
      main = paste0("Fishing Effort of ", common_name, " (", sci_name, ")"),
      legend = TRUE,
      plg = list(title = "Log Fishing Hours"),
      cex.main = 2
    )
    map("world", add = TRUE)
    
    mtext(
      paste0(
        "Figure ", i, ": Summed log-scale fishing hours of ",
        common_name, " (", sci_name, "), masked to the upper quartile of habitat suitability. ",
        "The map highlights\nareas the species is most impacted by the following gears: ",
        gear_list, "."
      ),
      side = 1,
      line = 4,
      cex = 1.1
    )
    
    dev.off()
    cat("  Successfully created:", pdf_filename, "\n")
    
  }, error = function(e) {
    cat("  Error processing", common_name, ":", e$message, "\n")
  })
  
}

cat("\nLoop complete! Check outputs folder.\n")