#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(terra)
  library(future)
  library(future.apply)
})

in_dir  <- "data/gfw_txt"
out_dir <- "data/gfw_rs"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

gfw_txt <- list.files(in_dir, pattern="\\.txt$", full.names=TRUE)

make_out_path <- function(in_path) {
  base <- basename(in_path)
  base <- sub("^agg_cell_", "", base)
  base <- sub("_full\\.txt$", "", base)
  base <- gsub("_", "-", base)
  file.path(out_dir, paste0("agg_cell_", base, ".rds"))
}

# Canonical 0.01 global grid
tmpl_001 <- rast(ncols=36000, nrows=18000,
                 xmin=-180, xmax=180,
                 ymin=-90,  ymax=90,
                 crs="EPSG:4326")

terraOptions(progress=1, memfrac=0.8, todisk=TRUE)

# Linux HPC: multicore is best here
plan(multicore, workers = 8)

future_lapply(gfw_txt, function(f) {
  df <- read.table(f, header=TRUE)
  
  r <- rast(df[, c("lon","lat","fishing_hours_sum")],
            type="xyz",
            crs="EPSG:4326")
  rm(df); gc()
  
  r_global <- resample(r, tmpl_001, method="near")
  r_005    <- aggregate(r_global, fact=5, fun="sum", na.rm=TRUE)
  
  out_rds <- make_out_path(f)
  saveRDS(r_005, out_rds)
  out_rds
})

message("[gfw] done")