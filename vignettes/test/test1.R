library(dscrutils)

# Run the DSC.
system("dsc temp1.dsc")

# Load the DSC results.
out <- dscquery("temp1","gen.x")
