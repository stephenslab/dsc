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

test_that("dscread generates an error when both R and Python files are found",{
  dsc.dir1 <- system.file("datafiles","dscread_test_files","R+python",
                          package = "dscrutils")
  expect_error(dscread(dsc.dir,"t_1"))
})
          
