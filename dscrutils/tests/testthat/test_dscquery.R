context("dscrutils")

test_that(paste("First one_sample_location DSC query examples returns",
                "a 40 x 7 data frame"),{

  # Retrieve results from the "one_sample_location" DSC experiment in
  # which the true mean is 1. The MSE (mean squared error) values
  # should be extracted into the "mse.mse" column.
  dsc.dir <- system.file("datafiles","one_sample_location","dsc_result",
                         package = "dscrutils")
  dat <- dscquery(dsc.dir,targets = c("simulate.n","analyze","score.error"),
                  conditions = "$(simulate.n) > 10",verbose = FALSE)
  expect_equal(dim(dat),c(40,7))
})

test_that(paste("Filtering by conditions argument for one_sample_location",
                "DSC query gives same result as filtering in with subset"),{

  # Retrieve results from all simulations.                  
  dsc.dir <- system.file("datafiles","one_sample_location","dsc_result",
                         package = "dscrutils")
  dat1 <- dscquery(dsc.dir,targets = "simulate.n analyze score.error",
                   verbose = FALSE)

  # Retrieve results only for simulations in which the "mean" module
  # was run.
  dat2 <- dscquery(dsc.dir,targets = "simulate.n analyze score.error",
                   conditions = "$(analyze) == 'mean'",verbose = FALSE)
  expect_equal(subset(dat1,analyze == "mean"),dat2,
               check.attributes = FALSE)

  # Retrieve results only for simulations in which the error summary
  # is greater than 0.25.
  dat3 <- dscquery(dsc.dir,targets = "simulate.n analyze score.error",
                   conditions = "$(score.error) > 0.25",verbose = FALSE)
  expect_equal(subset(dat1,score.error > 0.25),dat3,
               check.attributes = FALSE)

  # Retrieve the DSC results only for simulations in which the "mean"
  # module was run, and which which the error summary is greater than
  # 0.25.
  dat4 <- dscquery(dsc.dir,targets = "simulate.n analyze score.error",
                  conditions = c("$(score.error) > 0.25",
                                 "$(analyze) == 'median'"),
                   verbose = FALSE)
  expect_equal(subset(dat1,analyze == "median" & score.error > 0.25),
               dat4,check.attributes = FALSE)
})

test_that("ash DSC query example returns a 10 x 6 data frame",{

  # Retrieve some results from the "ash" DSC experiment. In this
  # example, the beta estimates are long vectors (of length one
  # thousand), so the results are not returned in a data frame.
  dsc.dir <- system.file("datafiles","ash","dsc_result",package = "dscrutils")
  dat <- dscquery(dsc.dir,
           targets = c(paste("simulate",c("nsamp","g"),sep="."),
                       paste("shrink",c("mixcompdist","beta_est","pi0_est"),
                             sep=".")),
           conditions = "$(simulate.g) =='list(c(2/3,1/3),c(0,0),c(1,2))'",
           verbose = FALSE)
  expect_false(is.data.frame(dat))
  expect_equal(length(dat),6)
})

test_that(paste("Second ash DSC example without shrink.beta_est",
                "returns a data frame"),{

  # This is the same as the previous example, but extracts the
  # vector-valued beta estimates into the outputted data frame. As a
  # result, the data frame of query results is much larger (it has over
  # 1000 columns).
  dsc.dir <- system.file("datafiles","ash","dsc_result",package = "dscrutils")
  dat <- dscquery(dsc.dir,
           targets = c("simulate.nsamp","simulate.g","shrink.mixcompdist",
                       "shrink.pi0_est"),
           conditions ="$(simulate.g)=='list(c(2/3,1/3),c(0,0),c(1,2))'",
           verbose = FALSE)
  expect_true(is.data.frame(dat))
  expect_equal(dim(dat),c(10,5))
})

test_that(paste("Second one_sample_location DSC example returns an error",
                "because mse.score does not exist"),{

  # This query should generate an error because there is no output
  # called "score" in the "mse" module.
  dsc.dir <- system.file("datafiles","one_sample_location","dsc_result",
                         package = "dscrutils")
  expect_error(dscquery(dsc.dir,
                        targets = c("simulate.n","analyze","score.mse"),
                        conditions = "$(simulate.n) > 10",
                        verbose = FALSE))
})

test_that(paste("dscquery appropriately handles unassigned targets when",
                "other targets are scalars"),{
  dat <- data.frame(DSC                     = c(1,2,1,2),
                    sim_params.params_store = c(NA,NA,5,5),
                    cause.z                 = c(0.25,0.25,NA,NA))
  dsc.dir <- system.file("datafiles","misc","results1",package = "dscrutils")
  out <- dscquery(dsc.dir,
                  targets.notreq = c("sim_params.params_store","cause.z"),
                  verbose = FALSE)
  expect_equal(dat,out)
  expect_equal(is.na(dat),is.na(out))
})

test_that(paste("dscquery appropriately handles unassigned targets when",
                "other targets are vectors"),{
  dat <- list(DSC                     = list(1,2,1,2),
              sim_params.params_store = list(NA,NA,1:20,1:20),
              cause.z                 = list(0.25,0.25,NA,NA))
  dsc.dir <- system.file("datafiles","misc","results2",package = "dscrutils")
  out <- dscquery(dsc.dir,
                  targets.notreq = c("sim_params.params_store","cause.z"),
                  verbose = FALSE)
  expect_equal(dat,out)
})
