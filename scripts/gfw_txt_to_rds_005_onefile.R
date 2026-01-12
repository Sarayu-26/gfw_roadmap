#!/usr/bin/env Rscript
suppressPackageStartupMessages(library(terra))

i <- as.integer(commandArgs(trailingOnly = TRUE)[1])
if (is.na(i)) stop("Need SLURM index argument")

in_dir  <- "data/gfw_txt"
out_dir <- "data/gfw_rs"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

files <- list.files(in_dir, pattern="\\.txt$", full.names=TRUE)
f <- files[i]
if (is.na(f)) stop("Index out of range: ", i)

tmpl_001 <- rast(ncols=36000, nrows=18000,
                 xmin=-180, xmax=180, ymin=-90, ymax=90,
                 crs="EPSG:4326")

make_out_path <- function(in_path) {
  base <- basename(in_path)
  base <- sub("^agg_cell_", "", base)
  base <- sub("_full\\.txt$", "", base)
  base <- gsub("_", "-", base)
  file.path(out_dir, paste0("agg_cell_", base, ".rds"))
}

df <- read.table(f, header=TRUE)
r  <- rast(df[, c("lon","lat","fishing_hours_sum")], type = "xyz", crs = "EPSG:4326")

r_global <- resample(r, tmpl_001, method="near")
r_005    <- aggregate(r_global, fact = 5, fun = "sum", na.rm = TRUE)

saveRDS(r_005, make_out_path(f))