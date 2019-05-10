context("dscrutils")

test_that("Contents of Python and R output files are the same",{
  dsc.dir1 <- system.file("datafiles","dscread_test_files","R",
                          package = "dscrutils")
  dsc.dir2 <- system.file("datafiles","dscread_test_files","python",
                          package = "dscrutils")
  dat1     <- dscread(dsc.dir1,"t_1")
  dat2     <- dscread(dsc.dir2,"t_1")
  expect_equal(sort(names(dat1)),sort(names(dat2)))
})
