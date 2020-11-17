#' @title Read DSC Module Outputs
#'
#' @description Reads in DSC module outputs generated from a single
#' run of a module instance.
#'
#' @details DSC module outputs are either stored in RDS files (see
#' \code{\link{readRDS}}) or a Python "pickle" file. For DSC module
#' outputs stored as Python pickle files, the reticulate package is
#' used to import the data into R.
#' 
#' @param outdir Directory where the DSC output is stored.
#'
#' @param outfile File specifying the file path relative to the DSC
#' directory. You can use \code{\link{dscquery}} with the
#' \code{module.output.file} to obtain a correct file path. Note that
#' the file path should not contain the file extension (".rds" or
#' ".pkl").
#'
#' @return The return file is a list containing the DSC module
#' outputs. This list always includes a "DSC_DEBUG" list element
#' containing additional information recorded by DSC, such as the
#' replicate id.
#'
#' @seealso \code{\link{dscquery}}
#' 
#' @examples
#'
#' dsc.dir <- system.file("datafiles","one_sample_location",
#'                        "dsc_result",package = "dscrutils")
#' dat <- dscquery(dsc.dir,targets = "simulate",
#'                 module.output.file = "simulate")
#' out <- dscread(dsc.dir,dat$simulate.output.file[1])
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

    # This additional processing step is needed to convert more
    # complex Python data structures such as a pandas data frames.
    out <- rapply(out,reticulate::py_to_r,classes = "python.builtin.object",
                  how = "replace")
  } else {
    warning(sprintf(paste("Unable to read from %s as one or",
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

