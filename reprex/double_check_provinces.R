library(terra)

rs <- rast("outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q3_pct.tif")
rs_q3 <- rast("outputs/fsle_quartiles_global_provinces/fsle_quartiles_1994_2022_Q3_pct.tif")
plot(rs)
plot(rs_q3)

rs_q3[] <- ifelse(rs_q3[] >= 75, rs_q3[], NA)
plot(rs_q3)




q1  <- rast("outputs/fsle_quartiles_global_provinces/new/fsle_quartiles_1994_2022_Q1_pct.tif")
q2  <- rast("outputs/fsle_quartiles_global_provinces/new/fsle_quartiles_1994_2022_Q2_pct.tif")
q3  <- rast("outputs/fsle_quartiles_global_provinces/new/fsle_quartiles_1994_2022_Q3_pct.tif")
den <- rast("outputs/fsle_quartiles_global_provinces/new/fsle_quartiles_1994_2022_valid_days.tif")

sum_pct <- q1 + q2 + q3
global(mask(sum_pct, den > 0), "range", na.rm=TRUE)
global(mask(sum_pct, den > 0), "mean",  na.rm=TRUE)


global(q3, fun = function(x) stats::quantile(x, probs = c(0.5, 0.9, 0.95, 0.99), na.rm = TRUE))

q2_filter <- mask(q2, ifel(q3 >= 50, 1, NA))
q3_filter <- mask(q3, ifel(q3 >= 50, 1, NA))

plot(q2_filter, col = hcl.colors(100, "viridis"))


plot(q3)
plot(rast("outputs/fsle_quartiles_global/fsle_quartiles_1994_2022_Q3_pct.tif"))
