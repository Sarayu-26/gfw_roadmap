library(terra)
library(dplyr)
library(stringr)
library(maps)


# 1 step reading GFW and raster  ------------------------------------------

asdf <- function(indir, 
                 outdir, 
                 res,
                 crs = NULL)

gfw.txt <- list.files(path = "outputs", 
                      pattern = ".txt", 
                      all.files = TRUE,
                      full.names = TRUE, 
                      recursive = FALSE)
gfw.txt <- gfw.txt[13:15]

for(i in seq_along(gfw.txt)){
  df <- read.csv(gfw.txt[i], sep = "\t") %>% 
    as_tibble() %>% 
    dplyr::select(-gear)  
  pts <- vect(df, geom = c("lon", "lat"), crs = "EPSG:4326")
  rs_blank <- rast(res = 0.5, crs = "EPSG:4326")
  rs_agg1 <- rasterize(
    pts, 
    rs_blank,
    field = "fishing_hours_sum", # variable, in this case is fihsing hours
    fun = "sum",      # aggregation function
    background = NA    # fill empty cells with 0
  )
  nms <- stringr::str_remove_all(basename(gfw.txt[i]), ".txt")
  saveRDS(rs_agg1, paste0("outputs/gfw_rs/", nms, ".rds"))
}

rs_trollers <- readRDS("outputs/gfw_rs/agg_cell_trollers_full.rds")
plot(log10(rs_trollers))

rs_squid <- readRDS("outputs/gfw_rs/agg_cell_squid_jigger_full.rds")
plot(log10(rs_squid))


# 2 -----------------------------------------------------------------------

info_csv <- read.csv("data/meta_species.csv") %>% 
  as_tibble()

sdm.Rdata <- list.files(path = "data", 
                      pattern = "*SP.*.Rdata", 
                      all.files = TRUE,
                      full.names = TRUE, 
                      recursive = FALSE)

nm <- stringr::str_remove_all(basename(sdm.Rdata[1]), ".Rdata")
nm <- unlist(stringr::str_split(nm, "_"))[5]

info_csv01 <- info_csv %>% 
  dplyr::filter(AphiaID == nm)

gtt <- unique(info_csv01$GFWGearType)
gtt <- unlist(stringr::str_split(gtt, ", "))

gfw_rs <- list.files(path = "outputs/gfw_rs", 
           pattern = ".rds", 
           all.files = TRUE, 
           full.names = TRUE, 
           recursive = FALSE)


matches <- sapply(gtt, function(g) grep(g, gfw_rs, value = TRUE))
files_true <- unlist(matches[lengths(matches) > 0], use.names = FALSE)

rs_files_true <- rast(files_true)


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

rs_FF <- mask(rs_files_true, rs_105797)
plot(log(rs_FF))
map("world", add = TRUE)

plot(log(rs_105797))
map("world", add = TRUE)
