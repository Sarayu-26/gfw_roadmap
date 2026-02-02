library(terra)
library(maps)

count_per_pixel_across_dirs <- function(dirs,
                                        pattern = "\\.tif$",
                                        recursive = FALSE,
                                        cores = 1,
                                        out_tif = NULL) {
  
  tif_files <- unlist(lapply(dirs, function(d) {
    list.files(d, pattern = pattern, full.names = TRUE, recursive = recursive)
  }), use.names = FALSE)
  
  if (length(tif_files) == 0) {
    stop("No .tif files found in: ", paste(dirs, collapse = ", "))
  }
  
  message("Total .tif files found: ", length(tif_files))
  
  r <- rast(tif_files)
  n_per_cell <- app(r, fun = function(x) sum(!is.na(x)), cores = cores)
  
  if (!is.null(out_tif)) {
    writeRaster(n_per_cell, out_tif, overwrite = TRUE)
  }
  
  n_per_cell
}

# Combine BirdLife + SDMs (add more folders later if you want)
dirs <- c("outputs/birdlife_gfw", "outputs/sdms_gfw")

rs_all <- count_per_pixel_across_dirs(
  dirs = dirs,
  cores = 3,
  out_tif = "outputs/count_per_pixel_birdlife_plus_sdms.tif"
)

# plot(rs_all)
# map("world", add = TRUE)
