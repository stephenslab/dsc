# Small test of dscquery function in R package.
source("R/dscquery.R")
out <- dscquery("inst/datafiles/one_sample_location/dsc_result",
                targets = c("simulate.n","estimate","mse.score"),
                condition = "simulate.true_mean = 1")
