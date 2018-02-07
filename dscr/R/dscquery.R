#' @title SQL-like interface for querying DSC output.
#'
#' @description Add more detailed (paragraph-length) description here.
#' 
#' @param dsc.outdir Directory where DSC output is stored.
#' 
#' @param targets Query targets specified as a character vector, e.g.,
#' \code{targets = c("simulate.n","estimate","mse.score")}. These will
#' be the names of the columns in the returned data frame.
#'
#' @param condition The default \code{NULL} means "no condition", in
#' which case the results for all DSC pipelines are returned.
#'
#' @param groups Definition of module groups. Currently
#' 
#' @param add.path If TRUE, the returned data frame will contain full
#' pathnames, not just the base filenames.
#' 
#' @param exec The command or pathname of the dsc-query executable.
#'
#' @param verbose If \code{verbose = TRUE}, print progress of DSC
#' query command to the console.
#'
#' @return Add description of return value here.
#'
#' @note May not work in Windows.
#'
#' @examples
#' // Add an example here.
#' 
#' @export
dscquery <- function (dsc.outdir, targets, condition = NULL, groups,
                      add.path = FALSE, exec = "dsc-query",
                      verbose = TRUE) {

  # CHECK INPUTS
  # ------------
  # Check input argument "dsc.outdir".
  if (!(is.character(dsc.outdir) & length(dsc.outdir) == 1))
    stop("Argument \"dsc.outdir\" should be a character string")
    
  # Check input argument "targets".
  if (!(is.character(targets) & is.vector(targets)))
    stop("Argument \"targets\" should be a character vector")

  # Check input argument "condition".
  if (!is.null(condition))
    if (!(is.character(condition) & length(condition) == 1))
      stop(paste("Argument \"condition\" should be NULL or a single",
                 "character string"))
    
  # Check input argument "groups".
  if (!is.missing(groups))
    stop("Argument \"groups\" is not yet implemented")

  # Check input argument "add.path".
  if (!(is.logical(add.path) & length(add.path) == 1))
    stop("Argument \"add.path\" should be TRUE or FALSE")

  # Check input argument "exec".
  if (!(is.character(exec) & length(exec) == 1))
    stop("Argument \"exec\" should be a character string")

  # Check input argument "verbose".
  if (!(is.logical(verbose) & length(verbose) == 1))
    stop("Argument \"verbose\" should be TRUE or FALSE")

  # RUN DSC QUERY COMMAND
  # ---------------------
  # Build the command based on the inputs.
  cmd.str <- paste(exec,"--add-path",add.path,"--targets",
                   paste(targets,collapse = " "))
  if (!is.null(condition))
    cmd.str <- paste(cmd.str,"--condition",condition)
  cmd.str <- paste(cmd.str,dsc.outdir)
  browser
  system(cmd.str)
}
