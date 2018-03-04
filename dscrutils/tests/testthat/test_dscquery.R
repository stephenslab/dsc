context("dscrutils")

test_that("one_sample_location DSC query example returns a 40 x 5 data frame",{

  # Retrieve results from the "one_sample_location" DSC experiment in
  # which the true mean is 1. The MSE (mean squared error) values
  # should be extracted into the "mse.mse" column.
  dsc.dir <- system.file("datafiles","one_sample_location",
                         "dsc_result",package = "dscrutils")
  dat <- dscquery(dsc.dir,targets = c("simulate.n","estimate","mse.mse"),
                  condition = "simulate.true_mean = 1")
  expect_equal(dim(dat),c(20,5))
})
