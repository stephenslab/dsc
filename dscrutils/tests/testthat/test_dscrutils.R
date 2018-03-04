context("dscrutils")

test_that("one_sample_location DSC query example returns a 40 x 5 data frame",{
  example("dscquery")
  expect_equal(dim(dat),c(40,5))
})
