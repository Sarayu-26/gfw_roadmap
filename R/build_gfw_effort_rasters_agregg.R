library(terra)
library(future)
library(future.apply)

dir.create("data/gfw_rs", recursive = TRUE, showWarnings = FALSE)

gfw_txt <- list.files("data/gfw_txt", pattern="\\.txt$", full.names=TRUE)

plan(multicore, workers = 5)

make_out_path <- function(in_path) {
  base <- basename(in_path)
  base <- sub("^agg_cell_", "", base)
  base <- sub("_full\\.txt$", "", base)
  base <- gsub("_", "-", base)
  file.path("outputs/gfw_rs", paste0("agg_cell_", base, ".rds"))
}

future_lapply(gfw_txt, function(f) {
  
  # create template INSIDE worker (avoids invalid external pointer)
  tmpl_001 <- rast(
    ncols=36000, nrows=18000,
    xmin=-180, xmax=180,
    ymin=-90,  ymax=90,
    crs="EPSG:4326"
  )
  
  df <- read.table(f, header = TRUE)
  
  r <- rast(df[, c("lon","lat","fishing_hours_sum")],
            type="xyz",
            crs="EPSG:4326")
  rm(df); gc()
  
  r_global <- resample(r, tmpl_001, method="near")
  r_005    <- aggregate(r_global, fact = 5, fun = "sum", na.rm=TRUE)
  
  out_rds <- make_out_path(f)
  saveRDS(r_005, out_rds)
  
  out_rds
})
