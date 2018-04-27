library(dscrutils)

# Run the DSC.
system("dsc test1.dsc")

# Query the DSC results.
out <- dscquery("test1",c("gen.x","gen.m","gen.r"))

