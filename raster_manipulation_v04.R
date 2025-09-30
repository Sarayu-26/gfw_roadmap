library(terra)
library(maps)


#sketching what a 4 loop process can look like 

#example with about 3 species 
#species_ids <- c(105797, 105789, 105792, 105794) 
#in order: sandbar shark, silky shark, bull shark, oceanic whitetip shark

test_files <-sdm.Rdata[2:4]

# Loop over all species SDM files
for (file in sdm.Rdata) {
  
  # --- Extract species AphiaID from filename ---
  nm <- stringr::str_remove_all(basename(file), ".Rdata")
  aphia_id <- unlist(stringr::str_split(nm, "_"))[5]
  
  # --- Get metadata for this species ---
  info_csv01 <- info_csv %>%
    dplyr::filter(AphiaID == aphia_id)
  
  gtt <- unique(info_csv01$GFWGearType)
  gtt <- unlist(stringr::str_split(gtt, "_"))
  
  # --- Find matching fishing effort rasters ---
  files_true <- gfw_rs[basename(gfw_rs) %in% paste0("agg_cell_", gtt, ".rds")]
  rs_files_true_FF <- lapply(files_true, rast) %>% rast()
  rs_files_true_FF <- terra::app(rs_files_true_FF, fun = sum, na.rm = TRUE)
  
  # --- Read SDM and rasterize ---
  SP <- read_sdm(file) %>% 
    dplyr::select(x, y, Current) %>% 
    as_tibble()
  
  rs_sdm <- vect(SP, geom = c("x", "y"), crs = "EPSG:4326")
  rs_blank <- rast(res = 0.5, crs = "EPSG:4326")
  rs_sdm <- rasterize(rs_sdm, rs_blank, field = "Current", fun = "sum", background = NA)
  
  # --- Keep only upper quartile habitat ---
  qtl <- as.vector(quantile(rs_sdm[], na.rm = TRUE)[4])
  rs_sdm[] <- ifelse(rs_sdm[] >= qtl, rs_sdm[], NA)
  
  # --- Mask fishing effort with habitat ---
  rs_FF <- mask(rs_files_true_FF, rs_sdm)
}