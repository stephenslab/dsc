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
  outfile <- path.expand(file.path(outdir,outfile))
  rds     <- paste0(outfile,".rds")
  pkl     <- paste0(outfile,".pkl")
  if (file.exists(rds) & file.exists(pkl))
    stop(sprintf(paste("Both %s and %s DSC output files exist; files should",
                       "be cleaned up by running \"dsc --clean\""),rds,pkl))
  else if (file.exists(rds))

    # Read from the .rds file.
    out <- tryCatch(readRDS(rds),
      error = function (e) {
        warning(sprintf("Unable to read from %s; file may be corrupted",rds))
        return(NULL)
      })
  else if (file.exists(pkl)) {

    # Read from the .pkl file.
    if (!requireNamespace("reticulate",quietly = TRUE))
      stop("Cannot read from .pkl file due to missing reticulate package")
    out <- tryCatch(reticulate::py_load_object(pkl),
      error = function (e) {
        warning(sprintf("Unable to read from %s; file may be corrupted",pkl))
        return(NULL)
      })

    # This additional processing step may not be needed:
    #
    #   out <- rapply(out,reticulate::py_to_r,
    #                 classes = "python.builtin.object",
    #                 how = "replace")
    #
  } else {
    warning(sprintf(paste("Unable to read from DSC output file %s as one or",
                          "more files may be missing; returning NULL"),
                    outfile))
    out <- NULL
  }
  
  # We may use this code in the future to read from YAML files:
  #
  #   yaml.load_file(outfile)
  #
  return(out)
}

