context("dscrutils")

test_that(paste("dscreval generates correct tuple-like string",
                "representations of various expression inputs"),{

  # Generates a string encoding a tuple of numeric values.
  x <- dscreval("c(3,-1,14)")
  expect_equal(x,"3,-1,14")

  # Generates a string encoding a tuple of numeric values.
  x <- dscreval("seq(1,2,length.out = 5)")
  expect_equal(x,"1,1.25,1.5,1.75,2")

  # Generates a string encoding a tuple of logical values.
  x <- dscreval("1:7 < 5")
  expect_equal(x,"TRUE,TRUE,TRUE,TRUE,FALSE,FALSE,FALSE")

  # Generates a string encoding a tuple of character values.
  x <- dscreval("c('Illinois','Michigan','Ohio')")
  expect_equal(x,'"Illinois","Michigan","Ohio"')

  # Generates a string encoding of a nested tuple, in which the first
  # element contains character values, and the second element contains
  # numeric values.
  x <- dscreval("list(x = LETTERS[1:3],y = 1:3)")
  expect_equal(x,'("A","B","C"),(1,2,3)')

  # Produces an error because NULL is not allowed.
  expect_error(dscreval("NULL"))

  # Produces an error because complex numbers are not allowed.
  expect_error(dscreval("polyroot(c(-1,2,-1,4))"))

  # Produces an error because NULL values are not allowed anywhere in
  # the data structure.
  expect_error(dscreval("vector('list',3)"))
  
  # Produces an error because lists containing lists are not allowed.
  expect_error(dscreval("list(x = LETTERS[1:5],y = 1:5,z = as.list(1:5))"))
})


