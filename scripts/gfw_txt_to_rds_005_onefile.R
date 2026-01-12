#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(terra)
  library(future)
  library(future.apply)
})

in_dir  <- normalizePath("data/gfw_txt", mustWork = TRUE)
out_dir <- normalizePath("data/gfw_rs", mustWork = FALSE)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

gfw_txt <- list.files(in_dir, pattern="\\.txt$", full.names=TRUE)
stopifnot(length(gfw_txt) > 0)

make_out_path <- function(in_path) {
  base <- basename(in_path)
  base <- sub("^agg_cell_", "", base)
  base <- sub("_full\\.txt$", "", base)
  base <- gsub("_", "-", base)
  file.path(out_dir, paste0("agg_cell_", base, ".tif"))
}

tmpl_001 <- rast(
  ncols=36000, nrows=18000,
  xmin=-180, xmax=180,
  ymin=-90,  ymax=90,
  crs="EPSG:4326"
)

# Use a persistent temp dir on HPC if possible (prevents weirdness)
terraOptions(progress=1, memfrac=0.8, todisk=TRUE, tempdir=file.path(out_dir, "terra_tmp"))
dir.create(file.path(out_dir, "terra_tmp"), showWarnings = FALSE, recursive = TRUE)

plan(multicore, workers = 8)

res <- future_lapply(
  gfw_txt,
  function(f) {
    out_tif <- make_out_path(f)
    
    # skip only if it looks real
    if (file.exists(out_tif) && file.info(out_tif)$size > 10^6) return(out_tif)
    
    tryCatch({
      df <- read.table(f, header = TRUE)
      stopifnot(all(c("lon","lat","fishing_hours_sum") %in% names(df)))
      
      r <- rast(df[, c("lon","lat","fishing_hours_sum")], type="xyz", crs="EPSG:4326")
      rm(df); gc()
      
      r_global <- resample(r, tmpl_001, method="near")
      r_005    <- aggregate(r_global, fact=5, fun="sum", na.rm=TRUE)
      
      # write the actual raster values
      writeRaster(r_005, out_tif, overwrite=TRUE)
      out_tif
    }, error = function(e) {
      errfile <- sub("\\.tif$", ".error.txt", out_tif)
      writeLines(paste("ERROR:", conditionMessage(e)), errfile)
      stop(e)
    })
  },
  future.seed = TRUE
)

message("[gfw] done. Wrote ", length(res), " rasters.")