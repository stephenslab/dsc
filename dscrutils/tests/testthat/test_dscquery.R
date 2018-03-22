context("dscrutils")

test_that(paste("First one_sample_location DSC query example returns",
                "a 40 x 7 data frame"),{

  # Retrieve results from the "one_sample_location" DSC experiment in
  # which the true mean is 1. The MSE (mean squared error) values
  # should be extracted into the "mse.mse" column.
  dsc.dir <- system.file("datafiles","one_sample_location","dsc_result",
                         package = "dscrutils")
  dat <- dscquery(dsc.dir,targets = c("simulate.n","analyze","score.error"),
                  conditions = "simulate.n > 10")
  expect_equal(dim(dat),c(40,7))
})

test_that("ash DSC query example returns a 10 x 6 data frame",{

  # Retrieve some results from the "ash" DSC experiment. In this
  # example, the beta estimates are long vectors (length 1,000), so the
  # results are not extracted into the outputted data frame.
  dsc.dir <- system.file("datafiles","ash","dsc_result",package = "dscrutils")
  dat <- dscquery(dsc.dir,
           targets = c(paste("simulate",c("nsamp","g"),sep="."),
                       paste("shrink",c("mixcompdist","beta_est","pi0_est"),
                             sep=".")),
           conditions = paste("simulate.g =",
                              "'list(c(2/3,1/3),c(0,0),c(1,2))'"))
  expect_equal(dim(dat),c(10,6))
})

test_that(paste("Second ash DSC example with max.extract.vector = 1000",
                "returns a n x m data frame"),{
                    
  # This is the same as the previous example, but extracts the
  # vector-valued beta estimates into the outputted data frame. As a
  # result, the data frame of query results is much larger (it has over
  # 1000 columns).
  dsc.dir <- system.file("datafiles","ash","dsc_result",package = "dscrutils")
  dat <- dscquery(dsc.dir,
           targets = c("simulate.nsamp","simulate.g","shrink.mixcompdist",
                       "shrink.beta_est","shrink.pi0_est"),
           conditions = paste("simulate.g =",
                              "'list(c(2/3,1/3),c(0,0),c(1,2))'"),
           max.extract.vector = 1000)
  expect_equal(dim(dat),c(10,1005))
})

test_that(paste("Second one_sample_location DSC example returns an error",
                "because mse.score does not exist"),{

  # This query should generate an error because there is no output
  # called "score" in the "mse" module.
  dsc.dir <- system.file("datafiles","one_sample_location","dsc_result",
                         package = "dscrutils")
  expect_error(dscquery(dsc.dir,
                        targets = c("simulate.n","analyze","score.mse"),
                        conditions = "simulate.n > 10"))
})
