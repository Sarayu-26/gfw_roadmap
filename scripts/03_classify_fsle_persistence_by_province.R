library(terra)

front_freq <- rast("outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q3_pct.tif")
prov       <- rast("outputs/boundaries/longhurst_prov_id_fslegrid.tif")

# align check (important)
stopifnot(compareGeom(front_freq, prov, stopOnError = FALSE))

# get province IDs
prov_ids <- sort(unique(values(prov, mat = FALSE), na.rm = TRUE))

# province-specific 75th percentile of the global persistence map
q75 <- vapply(prov_ids, function(pid) {
  vv <- values(mask(front_freq, prov, maskvalues = pid, inverse = TRUE), mat = FALSE)
  vv <- vv[!is.na(vv)]
  if (length(vv) < 25) return(NA_real_)
  as.numeric(quantile(vv, 0.75, na.rm = TRUE, names = FALSE))
}, numeric(1))

range(q75, na.rm = TRUE)
length(unique(round(q75, 6)))

q75_r <- classify(prov, cbind(prov_ids, q75), others = NA)
front_freq_topQ <- ifel(front_freq >= q75_r, front_freq, NA)
plot(rotate(front_freq_topQ))



# THE LOOP STUFF INSTEAD OF VAPPLY
# q75 <- numeric(length(prov_ids))
# for (i in seq_along(prov_ids)) {
#   pid <- prov_ids[1]
#   
#   vv <- values(mask(front_freq, prov, maskvalues = pid, inverse = TRUE), mat = FALSE)
#   vv <- vv[!is.na(vv)]
#   
#   if (length(vv) < 25) {
#     q75[i] <- NA_real_
#   } else {
#     q75[i] <- quantile(vv, 0.75, na.rm = TRUE, names = FALSE)
#   }
# }
