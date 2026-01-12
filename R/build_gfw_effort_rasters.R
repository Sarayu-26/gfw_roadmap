library(terra)
library(future)
library(future.apply)

dir.create("outputs/gfw_rs", recursive = TRUE, showWarnings = FALSE)

gfw_txt <- list.files("outputs/gfw_txt", pattern="\\.txt$", full.names=TRUE)

plan(multisession, workers = 5)   # use multicore on Linux, multisession is fine on Mac

make_out_path <- function(in_path) {
  base <- basename(in_path)
  base <- sub("^agg_cell_", "", base)
  base <- sub("_full\\.txt$", "", base)
  base <- gsub("_", "-", base)
  file.path("outputs/gfw_rs", paste0("agg_cell_", base, ".rds"))
}

future_lapply(gfw_txt, function(f) {
  df <- read.table(f, header = TRUE)
  r  <- rast(df[, c("lon","lat","fishing_hours_sum")], type="xyz", crs="EPSG:4326")
  rm(df); gc()
  
  out_rds <- make_out_path(f)
  saveRDS(r, out_rds)
  out_rds
})