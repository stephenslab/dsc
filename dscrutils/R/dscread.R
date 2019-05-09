#' @title Add title here.
#'
#' @description Add description here.
#'
#' @detail Add more details here.
#' 
#' @param outdir Directory where the DSC output is stored.
#'
#' @param outfile Describe argument "outfile" here.
#'
#' @return Describe return value here.
#' 
#' @examples
#'
#' # Add examples here.
#' 
#' @importFrom tools file_ext
#' @importFrom yaml yaml.load_file
#' 
#' @export
#'
dscread <- function (outdir, outfile) {

  # CHECK & PROCESS INPUTS
  # ----------------------
  # Check input argument "dsc.outdir".
  if (!(is.character(dsc.outdir) & length(dsc.outdir) == 1))
    stop("Argument \"dsc.outdir\" should be a character vector of length 1")

  # Look for files with extensions "rds" and "pkl".
  outfile <- file.path(outdir,outfile)
  rds     <- paste0(outfile,".rds")
  pkl     <- paste0(outfile,".pkl")
  if (file.exists(rds) & file.exists(pkl))
    stop(sprintf(paste("Both %s and %s DSC files exist; DSC output files",
                       "should be cleaned up using \"dsc --clean\""),
                 rdsfile,pklfile))
  else if (file.exists(rds))
    out <- read_dsc(rds)
  else if (file.exists(pkl))
    out <- read_dsc(pkl)
  
  inext = file_ext(infile)
  if (inext == "") {
    for (item in c("rds", "pkl", "yml")) {
      if (file.exists(paste0(infile, ".", item))) {
        inext = item
        infile = paste0(infile, ".", item)
        break
      }
    }
  }
  if (inext == "")
      stop(paste("Cannot determine extension for input file", infile))
  if (inext == "pkl") {
    if (!requireNamespace("reticulate",quietly = TRUE))
      stop("Cannot read from .pkl file due to missing reticulate package")
    result = reticulate::py_load_object(infile)
    return(rapply(result, reticulate::py_to_r, classes = "python.builtin.object", how = "replace"))
    # yaml.load_file(infile))
  } else
    out <- readRDS(outfile)
  return(out)
}

