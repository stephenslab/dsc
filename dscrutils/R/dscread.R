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
#' @seealso \code{\link{dscquery}}
#' 
#' @examples
#'
#' # Add one or more examples building on 
#' 
#' @importFrom tools file_ext
#' @importFrom yaml yaml.load_file
#' 
#' @export
#'
dscread <- function (outdir, outfile) {

  # Check the input arguments.
  if (!(is.character(outdir) & length(outdir) == 1))
    stop("Argument \"outdir\" should be a character vector of length 1")
  if (!(is.character(outfile) & length(outfile) == 1))
    stop("Argument \"outfile\" should be a character vector of length 1")
  
  # Look for files with extensions "rds" and "pkl".
  outfile <- file.path(outdir,outfile)
  rds     <- paste0(outfile,".rds")
  pkl     <- paste0(outfile,".pkl")
  if (file.exists(rds) & file.exists(pkl))
    stop(sprintf(paste("Both %s and %s DSC output files exist; files should",
                       "be cleaned up by running \"dsc --clean\""),rds,pkl))

  # Attempt to read the data stored in the DSC output file.
  if (file.exists(rds))

    # Read from the .rds file.
    out <- tryCatch(readRDS(outfile),
      error = function (e) stop(sprintf(paste("Unable to read from %s;",
                                              "file may be corrupted"),rds)))
  else {

    # Read from the .pkl file.
    if (!requireNamespace("reticulate",quietly = TRUE))
      stop("Cannot read from .pkl file due to missing reticulate package")
    out <- tryCatch(reticulate::py_load_object(infile),
                    error = function (e) stop(""))
    out <- rapply(out,reticulate::py_to_r,classes = "python.builtin.object",
                  how = "replace")
  }
                
  # We may use this in the future to read from YAML files:
  #
  #   yaml.load_file(infile))
  #

  return(out)
}

