context("dscrutils")

test_that("dscreval generates correct tuple-like string representations",{

  # Generates a string encoding a tuple of numeric values.
  x1 <- dscreval("c(3,-1,14)")
  expect_equal(x1,"3,-1,14")
})

