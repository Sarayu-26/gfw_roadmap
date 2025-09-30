library(terra)
library(dplyr)
library(stringr)
library(maps)

# 1 step reading GFW and raster  ------------------------------------------

# asdf <- function(indir, 
#                  outdir, 
#                  res,
#                  crs = NULL)
# 
# gfw.txt <- list.files(path = "outputs", 
#                       pattern = ".txt", 
#                       all.files = TRUE,
#                       full.names = TRUE, 
#                       recursive = FALSE)
# # gfw.txt <- gfw.txt[13:15]
# 
# for(i in seq_along(gfw.txt)){
#   df <- read.csv(gfw.txt[i], sep = "\t") %>% 
#     as_tibble() %>% 
#     dplyr::select(-gear)  
#   pts <- vect(df, geom = c("lon", "lat"), crs = "EPSG:4326")
#   rs_blank <- rast(res = 0.5, crs = "EPSG:4326")
#   rs_agg1 <- rasterize(
#     pts, 
#     rs_blank,
#     field = "fishing_hours_sum", # variable, in this case is fihsing hours
#     fun = "sum",      # aggregation function
#     background = NA    # fill empty cells with 0
#   )
#   nms <- stringr::str_remove_all(basename(gfw.txt[i]), ".txt")
#   saveRDS(rs_agg1, paste0("outputs/gfw_rs/", nms, ".rds"))
# }
# 
# rs_trollers <- readRDS("outputs/gfw_rs/agg_cell_trollers_full.rds")
# plot(log10(rs_trollers))
# 
# rs_squid <- readRDS("outputs/gfw_rs/agg_cell_squid_jigger_full.rds")
# plot(log10(rs_squid))


# 2 -----------------------------------------------------------------------

#reading meta_species in data folder and converts to tibble for easy reading
#meta_species table needed to identify which fishing gears correspond to species
info_csv <- read.csv("data/meta_species.csv") %>% 
  as_tibble()

#Each .Rdata keeps a habitat suitability map for a species
#we only want files that represent species
sdm.Rdata <- list.files(path = "data", #lists files in data folder
                      pattern = "*SP.*.Rdata", #selecting for SP. and .Rdata files only
                      all.files = TRUE,
                      full.names = TRUE, #returning full paths 
                      recursive = FALSE)
#extracting species identifier from the from the first SDM file 
nm <- stringr::str_remove_all(basename(sdm.Rdata[1]), ".Rdata")
nm <- unlist(stringr::str_split(nm, "_"))[5] #isolating 5th underscore token which is the AphiaID

#filter the metadata for that species using the AphiaID
#now we have species' metadata including gear type used to catch that species
info_csv01 <- info_csv %>% 
  dplyr::filter(AphiaID == nm)

#extracting gear type from the metadata
gtt <- unique(info_csv01$GFWGearType)
gtt <- unlist(stringr::str_split(gtt, "_")) 
#if multiple gear types are connected with underscores, we split them into a vector
#so it will look like "set

gfw_rs <- list.files(path = "outputs/gfw_rs", 
           pattern = ".rds", 
           all.files = TRUE, 
           full.names = TRUE, 
           recursive = FALSE)


files_true <- gfw_rs[basename(gfw_rs) %in% paste0("agg_cell_", gtt, ".rds")]

rs_files_true_FF <- lapply(files_true, function(x) rast(x))
rs_files_true_FF <- rast(rs_files_true_FF)
rs_files_true_FF <- terra::app(rs_files_true_FF, fun = sum, na.rm = TRUE)

# 3  ----------------------------------------------------------------------

read_sdm <- function(file) {
  e <- new.env()
  load(file, envir = e)
  objname <- ls(e)[1]
  obj <- e[[objname]]
  attr(obj, "species_code") <- sub(".*SP_(\\d+)\\.Rdata$", "\\1", file)
  obj
}

SP_105790 <- read_sdm(sdm.Rdata[1]) %>% 
  dplyr::select(x, y, Current) %>% 
  as_tibble()

rs_105797 <- vect(SP_105790, geom = c("x", "y"), crs = "EPSG:4326")
rs_blank <- rast(res = 0.5, crs = "EPSG:4326")
rs_105797 <- rasterize(
  rs_105797, 
  rs_blank,
  field = "Current", # variable, in this case is fihsing hours
  fun = "sum",      # aggregation function
  background = NA    # fill empty cells with 0
)
plot(rs_105797)
qtl <- as.vector(quantile(rs_105797[], na.rm = TRUE)[4])
rs_105797[] <- ifelse(rs_105797[] >= qtl, rs_105797[], NA)
plot(rs_105797)

rs_FF <- mask(rs_files_true_FF, rs_105797)
plot(log(rs_FF), 
     main = "Fishing Effort of Galapagos Shark (log scale)",  # title                                   
     legend = TRUE)   
map("world", add = TRUE)

pdf("outputs/SP_105790.pdf", width = 30, height = 16)
plot(log(rs_FF),
     main = "Fishing Effort of Galapagos Shark (log scale)",
     legend = TRUE)
map("world", add = TRUE)
dev.off()
