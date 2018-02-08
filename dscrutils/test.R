# Small test of dscquery function in R package.
library(dscrutils)
out <- dscquery("inst/datafiles/one_sample_location/dsc_result",
                targets = c("simulate.n","estimate","mse.score"),
                condition = "simulate.true_mean = 1")
